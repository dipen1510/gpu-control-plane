# Design discussion — v1

Record of the design discussion between the tech lead and the architect
(2026-10-15; D17 and D18 added 2026-10-16) that produced the v1 decisions. For each decision: the question
that started it, the options we weighed, what we chose and why, and which
invariant from `CLAUDE.md` it protects.

ADR-0001 will formalize these decisions; this file keeps the reasoning in the
order we worked it out.

---

## Summary

| # | Area | v1 decision |
|---|---|---|
| D1 | Request shape | One instance per request; `gpu_count` ∈ {1,2,4,8}; fits on one node; `Idempotency-Key` required |
| D2 | Quota | 16 GPUs per tenant per GPU type; atomic check-and-reserve; `403 quota_exceeded` |
| D3 | No capacity | Fail fast: `503 insufficient_capacity` + `Retry-After` |
| D4 | Scheduler form | Library inside each API replica, same DB transaction as create |
| D5 | Node selection | Filter → best-fit score → tie-break → atomic reserve, fall back to next candidate |
| D6 | Per-node limit | `sum(gpu_count) ≤ capacity` (8); CPU/RAM fixed per GPU |
| D7 | Packing | Bin-packing + fragmentation metric |
| D8 | GPU assignment | Count only, no GPU indices |
| D9 | Database | Postgres 16 + outbox |
| D10 | Concurrency control | Optimistic concurrency (`resource_version`); DB conditional updates are the final guard |
| D11 | Source of truth | Postgres = customer state; Kubernetes = execution; idempotent transitions bridge them |
| D12 | Lifecycle | `202 Accepted` at SCHEDULED; controller drives the rest async; boot/shutdown delays simulated in the Pod; provisioning timeout + injected boot failures; billing from RUNNING |
| D13 | Leader election | Controller only |
| D14 | Fake GPUs | kind workers + label + extended resource `gpu.example.com/gpu: 8`; instance = Pod |
| D15 | Node inventory | Rows inserted once via Node sync; placement only updates `allocated`; single owner per column; drift reconciler |
| D16 | Client notifications | Clients poll `GET` with `If-None-Match` (304 when unchanged); push (webhook / SSE / watch) deferred to M3+ on the outbox and event bus |
| D17 | M1 lifecycle driver | M1 only: an instant driver in the apiserver walks the legal path via `store.Transition` (create → RUNNING, DELETE → TERMINATED); state machine unchanged; replaced by the M2 controller |
| D18 | Lessons from review (concurrency) | A lost `UPDATE` race keeps its row lock → one savepoint per reserve attempt; lock, then compare-and-set, then decrement (only the winner releases) |

---

## D1 — Request shape

**Question.** How many instances can a user ask for, and how big?

**Discussion.** Allowing arbitrary sizes or multi-node instances makes
placement much harder (gang scheduling, network topology). Batch create
("10 instances") is just a loop over single creates, so it adds nothing to
learn in v1.

**Decision.**
```
POST /v1/instances
Idempotency-Key: <client-generated>
{ "gpu_type": "H100" | "A100", "gpu_count": 1 | 2 | 4 | 8 }
```
One instance per request, and it must fit on a single node.

**Protects:** invariant 2 (idempotent create) via the required key.

---

## D2 — Quota

**Question.** What is each user's quota, and how is it enforced?

**Discussion.** Quota and capacity are different things:

| | Quota | Capacity |
|---|---|---|
| Answers | What is the tenant *allowed* to use? | What GPUs *exist* and are free? |
| Changes when | An admin changes the limit | Instances start/stop, nodes join/fail |
| Checked by | API server, synchronously | Placement (D5) |

The trap is a **check-then-act race**: two concurrent requests both read
"12 of 16 used", both pass, both insert 4 GPUs → 20 used. Checking and
reserving must be one atomic step.

**Decision.** Default **16 GPUs per tenant per GPU type**. In the create
transaction:
```sql
UPDATE tenant_quota
SET used = used + $n
WHERE tenant_id = $1 AND gpu_type = $2 AND used + $n <= quota_limit;
-- 0 rows → 403 quota_exceeded
```
The row lock serializes requests from the same tenant only; different tenants
don't block each other. `used` is decremented when an instance reaches
TERMINATED or FAILED. 403 tells the client retrying won't help.

**Protects:** quota limit under concurrency (a new invariant for ADR-0001).

---

## D3 — What happens when there is no capacity

**Question.** Should the API reject immediately or queue the request?

**Options.**
- **A. Fail fast** — return an error now. Simple; how most on-demand GPU
  clouds behave.
- **B. Queue** — accept as PENDING, place when capacity frees, FAIL after a
  timeout. Better for training jobs; requires a real scheduler with ordering.

**Decision.** **A for v1**, B in v2 (tech lead's call). Response:
`503 insufficient_capacity` with `Retry-After` and the GPU type, so clients
can tell it apart from a quota error.

**Consequence.** To answer immediately, placement must happen *inside the API
request*. This drives D4, D12 and D13.

---

## D4 — Scheduler as a library, not a service

**Question.** Is it the Kubernetes scheduler? How many run? Then: what does
"library, not a separate service" mean?

**Discussion — not the kube-scheduler.** Scheduling happens at two levels.
Our scheduler is fleet-level: it knows tenants, quotas, GPU types and our
capacity table, and decides "inst_abc → node-1". The kube-scheduler knows only
pods and nodes; we bypass it by pinning the Pod to the chosen node
(`spec.nodeName`). In production you'd also evaluate Kueue or Volcano; we
write our own to learn how they work.

**Discussion — library vs service.**

| | Library (v1) | Service (v2) |
|---|---|---|
| Where it runs | `internal/scheduler`, called as a function inside each API replica | Own binary and Deployment, called over the network |
| Fail fast | Answer in the same request and DB transaction | Extra network hop and failure mode |
| Things to operate | 1 | 2 + leader election |
| Correctness with 3 replicas | DB conditional update | Leader + DB conditional update |
| Global view for priority/fairness | No | Yes |

v1 has no queue, so no global view is needed. In v2, a small
`cmd/scheduler` adds leader election and a queue loop and calls the **same**
`scheduler.Place(...)` — no rewrite. Split a library into a service only when
a real need appears.

**Decision.**
```go
func (s *Scheduler) Place(ctx context.Context, tx pgx.Tx, req Request) (NodeID, error)
// ErrInsufficientCapacity when nothing fits
```
All API replicas place concurrently; correctness comes from D10.

---

## D5 — Node selection

**Question.** With 10 nodes that all have room, which one is picked?

**Discussion.** Same two-phase pattern as the kube-scheduler:

1. **Filter** — drop nodes with the wrong GPU type, not schedulable
   (cordoned, draining, missed heartbeats), or too few free GPUs.
2. **Score** — best fit: fewest free GPUs left after placement.
3. **Tie-break** — healthier node → spread one tenant's instances across
   failure domains (soft) → random (reduces contention between replicas).
4. **Reserve atomically**; on conflict try the next candidate.

Worked example — 10 H100 nodes, request for 2 GPUs:

| Node | Free | Filter | Free after | Rank |
|---|---|---|---|---|
| n1 | 8 | ✅ | 6 | 6th |
| **n2** | 2 | ✅ | **0** | **1st** |
| n3 | 5 | ✅ | 3 | 3rd |
| n4 | 8 | ✅ | 6 | 6th |
| n5 | 1 | ❌ too few | — | — |
| n6 | 4 | ✅ | 2 | 2nd |
| n7 | 8 | ❌ draining | — | — |
| n8 | 6 | ✅ | 4 | 4th |
| n9 | 8 | ✅ | 6 | 6th |
| n10 | 7 | ✅ | 5 | 5th |

n2 is chosen; n1, n4, n9 stay fully empty for future 8-GPU requests.

The scheduler decides from a possibly stale snapshot, then the database
confirms or refuses (optimistic concurrency):
```sql
UPDATE nodes SET allocated = allocated + $n
WHERE id = $node AND allocated + $n <= capacity AND schedulable;
-- 1 row → placed; 0 rows → try next candidate; none left → 503
```

**Decision.** Filter → best fit → tie-break → atomic reserve with fallback.
Filters and scorers are **pluggable functions**, so alternatives (spread,
rack preference, avoid flaky nodes) become small separate stories.

**Protects:** invariant 1.

---

## D6 — How many instances fit on one node

**Question.** A node runs multiple instances — what is the maximum?

**Discussion.** The limit is GPUs, not instance count. On an 8-GPU node:

| Mix | Instances |
|---|---|
| 8 × 1-GPU | 8 (maximum) |
| 4 × 2-GPU | 4 |
| 1 × 4-GPU + 2 × 2-GPU | 3 |
| 1 × 8-GPU | 1 |

CPU, RAM and network are also shared. Real clouds give each GPU a fixed
share, which keeps GPU count the only number to schedule. GPU splitting
(MIG) is out of scope.

**Decision.** `sum(gpu_count of active instances on node) ≤ capacity` (8).
CPU/RAM are a fixed share per GPU.

**Protects:** invariant 1.

---

## D7 — Bin-packing vs spreading

**Question.** Follows from D6: how do we avoid wasting capacity?

**Discussion.** **Fragmentation**: two nodes with 4 free GPUs each can't
serve an 8-GPU request even though 8 GPUs are free in total.

- **Bin-packing** (fullest node that fits) keeps whole nodes empty for big
  requests. Better for GPU clouds.
- **Spreading** (emptiest node) balances load and limits blast radius, but
  fragments capacity.

**Decision.** Bin-packing (D5 best fit). Emit a metric for fragmentation:
free GPUs that cannot serve an 8-GPU request.

---

## D8 — Count-based GPU assignment

**Discussion.** In real 8-GPU servers, GPUs are linked in groups (NVLink,
PCIe switches); a 4-GPU instance is faster on GPUs 0–3 than on 0,3,5,6. Real
clouds assign specific aligned indices (2-GPU at even slots, 4-GPU at 0 or 4),
like a buddy allocator.

**Decision.** v1 tracks counts only — enough to learn the concurrency
problems. Aligned index allocation is a v2 story.

---

## D9 — Postgres vs a key-value store

**Question.** Can we use a key-value DB instead of Postgres?

**Discussion.** What the design needs, by option:

| Need | Postgres | etcd | DynamoDB | Redis |
|---|---|---|---|---|
| Atomic multi-row write (state + outbox + capacity + quota) | ✅ | ✅ multi-key txn | ✅ ≤100 items | ⚠️ |
| Compare-and-swap | ✅ | ✅ native | ✅ | ⚠️ |
| Queries (by state, usage per tenant) | ✅ SQL | ❌ key/prefix only | ⚠️ indexes designed up front | ❌ |
| Change stream | outbox | ✅ watch | ✅ Streams | ⚠️ |
| Durable source of truth | ✅ | ✅ but small data (few GB) | ✅ | ❌ by default |
| Runs locally | ✅ | ✅ | emulator only | ✅ |

etcd is the most interesting alternative — Kubernetes is built on it, and our
`resource_version` pattern copies how Kubernetes uses etcd revisions — but it
is designed for small, hot control data, not billing history.

**Decision.** Postgres 16. State change and its event go into an **outbox**
table in the same transaction; a relay publishes them to NATS JetStream.

**Protects:** invariant 3 (no lost events).

---

## D10 — Concurrency control

**Discussion.** Two lessons from D2, D5 and leader election:
- Decide on a snapshot, then let the database confirm atomically
  (optimistic concurrency).
- **Never rely on a lock or a leader alone.** A paused leader (GC pause,
  network blip) can lose its lease and wake up still acting as leader
  (split brain). Conditional updates make that harmless.

**Decision.** Every row update carries `resource_version`:
```sql
UPDATE … SET …, resource_version = resource_version + 1
WHERE id = $1 AND resource_version = $2;   -- 0 rows → ErrStaleVersion
```
Capacity and quota use conditional updates (D2, D5). Correctness never
depends on leader election.

**Protects:** invariants 1 and 4.

---

## D11 — Who owns status: Postgres or etcd?

**Question.** Does the controller keep updating status in the DB or in etcd?

**Discussion.** There are two stores: Postgres (our instance record) and etcd
(behind the Kubernetes API, holding the `GPUInstance` resource and its Pod).
With two stores you must decide which is the truth for what. A distributed
transaction across both is not an option.

**Decision.**
- **Postgres** is the truth for the customer: what was requested, its state,
  what is billed.
- **Kubernetes** is the execution layer: runs things, reports what it sees.

Controller flow when it sees the Pod Running:
```
1. DB txn: UPDATE instances SET state='RUNNING', rv=rv+1
           WHERE id=$1 AND state='PROVISIONING'
           + INSERT outbox event "instance.running"
2. Update GPUInstance .status in Kubernetes
```
If the controller crashes between 1 and 2, the next reconcile repeats step 1;
`WHERE state='PROVISIONING'` makes it a no-op. **Idempotent transitions +
level-triggered loops** remove the need for a distributed transaction.

**Protects:** invariants 3 and 4.

---

## D12 — State machine and instance lifecycle

**Question.** In production an instance doesn't start right away: it is
initiated, in progress, running, terminating, terminated. Can we model that
without a queue, e.g. with a delay?

**Discussion — fail fast covers only the capacity decision.** "SCHEDULED"
does not mean running; it means GPUs are reserved on a node. The API answers
in milliseconds, and the rest of the lifecycle happens asynchronously in the
controller while the client polls `GET /v1/instances/{id}`:

```
POST /v1/instances → 202 Accepted { "state": "SCHEDULED" }

controller (background):
  SCHEDULED → PROVISIONING → RUNNING            (seconds to minutes)
DELETE:
  RUNNING → TERMINATING → TERMINATED            (seconds)
any of SCHEDULED / PROVISIONING → FAILED        (timeout or boot failure)
```

| Tech lead's term | State | Meaning |
|---|---|---|
| initiate | SCHEDULED | Accepted; quota and GPUs reserved on a node |
| in-progress | PROVISIONING | Controller created the Pod; it is booting |
| running | RUNNING | Pod ready; **billing starts here** |
| terminating | TERMINATING | Delete requested; Pod shutting down |
| terminated | TERMINATED | Pod gone; GPUs and quota released |

The response is **202 Accepted** (not 201) because the work is not finished.

**Discussion — where the delay lives.** Without a delay everything on kind
reaches RUNNING almost instantly and the in-between states and failure paths
are never exercised. Two places to put it:

- ❌ `time.Sleep` in the API handler or controller: blocks a request or a
  worker, the "progress" is lost if the process restarts, and it hides the
  real problem (waiting on an external system).
- ✅ **In the Pod (execution layer).** An init container runs
  `sleep $BOOT_SECONDS` (randomized, e.g. 10–30 s) to stand in for VM boot,
  image pull and GPU checks; a `preStop` hook sleeps a few seconds so
  TERMINATING is visible. The controller never sleeps: it reconciles, sees
  the Pod is not ready, and is woken by Kubernetes when the Pod changes. The
  delay is stored in the Pod's state, so a controller restart mid-boot loses
  nothing — level-triggered reconciliation.

**Decision.**
- Create returns **202 Accepted** with state SCHEDULED; the controller drives
  all later transitions asynchronously; clients poll.
- Boot and shutdown delays are simulated **in the Pod** (init container
  sleep, `preStop` sleep), never with sleeps in our code.
- **Provisioning timeout** (default 5 min): PROVISIONING too long → FAILED,
  Pod deleted, GPUs and quota released.
- **Injected boot failures:** `BOOT_FAIL_RATE` (e.g. 0.1) makes some Pods exit
  with an error so the FAILED path is exercised; the SRE raises it in chaos
  tests.
- **Delete during boot** is allowed (PROVISIONING → TERMINATING) and must be
  handled cleanly.
- Billing starts at RUNNING.
- PENDING stays in the model, unused in v1, for the v2 queue.

**M1 note.** The controller arrives in M2. Until then an *instant lifecycle
driver* in the apiserver walks the same states with no delays (D17); boot and
shutdown delays, the provisioning timeout and injected failures start with
the controller.

**Protects:** invariant 5 (capacity released on every path to a terminal
state) and invariant 3 (one outbox event per transition).

---

## D13 — Leader election

**Question.** What does "leader-elected" mean?

**Discussion.** Several replicas run for availability; one holds a
**Lease** (a Kubernetes object saying "replica-1 holds this until 10:00:15")
and renews it every few seconds. If it dies it stops renewing, the lease
expires (~15 s), and a standby takes over. Leader election gives efficiency
and a single decision-maker; it does not give correctness by itself (D10).

**Decision.** In v1 only the **controller** is leader-elected (3 replicas,
1 active). The scheduler gets leader election in v2 with the queue.

---

## D14 — Pods on kind "GPU" nodes

**Question.** What are Pods on kind "GPU" nodes?

**Discussion.** kind runs each Kubernetes node as a Docker container on the
laptop; there are no real GPUs. We fake them in three layers:

```
Mac → Docker
 ├── gpu-cp-control-plane   (API server, etcd, kube-scheduler)
 ├── gpu-cp-worker          label gpu.example.com/type=H100
 │                          capacity gpu.example.com/gpu = 8
 │    ├── Pod inst_abc (requests 4)
 │    └── Pod inst_def (requests 2)
 └── gpu-cp-worker2         label gpu.example.com/type=A100, capacity 8
```
1. **Label** — GPU type.
2. **Extended resource** — patch node status to advertise
   `gpu.example.com/gpu: 8`; Kubernetes tracks it like real GPUs (on real
   hardware, the NVIDIA device plugin advertises `nvidia.com/gpu`).
3. **Instance = Pod** running `sleep infinity`, requesting N of that
   resource, pinned to the chosen node.

At a real GPU cloud an instance is usually a VM or bare-metal host; the Pod
is a stand-in. The control-plane problems are the same.

Side benefit: Kubernetes tracks GPU usage independently of our `nodes` table,
so a Pod rejected for lack of GPUs reveals drift in our accounting.

**Decision.** As above. Extra fake nodes can exist as DB rows only, for
scheduler scale tests.

---

## D15 — Node inventory

**Question.** Is the node table pre-inserted, or does scheduling create
entries?

**Discussion.** The table is inventory. Rows are created once when a node
joins; scheduling only updates `allocated`.

| Event | Effect |
|---|---|
| Node joins | INSERT (once) |
| Instance placed | `allocated += n` (conditional) |
| Instance terminated/failed | `allocated -= n`, same txn as the state change |
| Node cordoned/unhealthy | `schedulable = false` |
| Node removed | keep row, `schedulable = false`; never delete while referenced |

Rows come from a **Node sync loop** in the controller that watches Kubernetes
Nodes and upserts type, capacity and schedulability — inventory is
discovered, not typed in. `make seed-nodes N=200` adds fake DB-only nodes for
scale tests.

**Two writers, one row.** The sync and placement both write `nodes`. If sync
rewrote the whole row it could overwrite `allocated` with a stale value. So
each column has **one owner**: sync owns `gpu_type`, `capacity`,
`schedulable`, `failure_domain`, `last_seen`; placement/termination own
`allocated`. Same idea as Kubernetes `spec` vs `status`.

**Backstop.** A periodic reconciler compares `allocated` with
`SUM(gpu_count)` of active instances per node and **alerts** on mismatch
rather than silently fixing it.

**Protects:** invariant 1.

---

## D16 — How clients learn about state changes

**Question.** Should we add the controller and SSE notifications to M1? And in
production, is SSE used, or does the dashboard call the status API?

**Discussion — SSE is easy, delivery is hard.** Pushing over SSE raises:
which API replica holds the client's connection (fan-out via Postgres
LISTEN/NOTIFY or NATS), how a client resumes after a disconnect without
missing a state (`Last-Event-ID` = outbox id; notifications are hints, the
outbox is the truth), ordering and duplicates (at-least-once, dedupe by event
id), heartbeats, per-tenant connection limits. Adding the controller to M1
would also grow M1 to ~20 stories.

**Discussion — what production does.** For instance state, polling is the
norm: consoles refresh lists on an interval, CLIs "wait" by polling (e.g.
`aws ec2 wait instance-running` polls every 15 s). Push is used for
automation through a separate channel (webhooks, event buses such as AWS
EventBridge state-change events), and Kubernetes clients use a watch from a
resourceVersion. SSE shines where updates are frequent (LLM token streams,
live logs). Polling wins here because state changes are rare, requests are
stateless (any replica answers), it works through every proxy, and GET always
reads the source of truth.

**Making polling cheap.** `GET` returns `ETag: "<rv>"`; the client sends
`If-None-Match: "<rv>"` and gets `304 Not Modified` when nothing changed.
Dashboards also pause polling when the tab is hidden.

**Decision.** No controller and no SSE in M1 (tech lead's call; the
controller stays in M2). Clients poll `GET /v1/instances/{id}` with
`If-None-Match`. Push notifications (webhook or SSE/watch) are deferred to M3+
and will be built from the outbox and event bus, never as a second source of
truth.

---

## D17 — M1 instant lifecycle driver

**Question.** (2026-10-16) In M1 nothing moves an instance past SCHEDULED, so
`DELETE` stops at TERMINATING and never releases GPUs or quota over HTTP. A
tenant who creates and deletes 16 GPUs is locked out for good. How do we close
the loop before the M2 controller exists?

**Options.**
- **A. Shortcut edges** — add edges such as `SCHEDULED → RUNNING` and
  `RUNNING → TERMINATED` (skipping TERMINATING) so the API jumps straight there.
  Smallest code, but M2 would have to *remove* edges (a breaking change to the
  state machine), and the outbox history would differ between M1 and M2
  (metering in M3 would see `scheduled → running` in old data and
  `scheduled → provisioning → running` in new data).
- **B. Instant driver** — a small component in the apiserver walks the
  *legal* path immediately, one `store.Transition` per step (each its own
  transaction, outbox row and rv check):
  ```
  after create commits:  SCHEDULED → PROVISIONING → RUNNING
  on DELETE:             <current> → TERMINATING → TERMINATED  (releases GPUs + quota)
  ```
  DELETE from SCHEDULED or PROVISIONING uses the existing `→ TERMINATING`
  edges. No sleeps. The state machine and the event history are identical to
  M2; only *who* calls `Transition` changes.

**Decision.** **B** (tech lead's call).
- API contract is the same in M1 and M2: `POST` returns `202` with the state
  at commit (SCHEDULED); a `GET` right after shows RUNNING. `DELETE` returns
  `202`; the instance ends TERMINATED. A client that "polls until RUNNING"
  works unchanged when M2 adds real delays.
- Final state stays TERMINATED (no DELETED state); rows are kept for billing
  history.
- Driver errors are logged; the instance stays in its last committed state.
- **Accepted M1 weakness:** if the apiserver crashes between the create commit
  and the driver's steps (or mid-delete), the instance is left in SCHEDULED /
  PROVISIONING / TERMINATING. Nothing repairs it in M1. The M2 controller's
  level-triggered reconcile fixes it, and the driver is deleted then.

**Protects:** invariant 3 (every step is its own transaction with exactly one
outbox row) and invariant 5 (capacity and quota released only in the
transaction that reaches TERMINATED).

---

## D18 — Lessons from review (concurrency)

**Question.** (2026-10-16) The verifier found two bugs that the ADR's own
text would have produced (S1-5, S1-9). Both are about what Postgres does
while one transaction *waits* for another. What should we remember?

Background for both: under READ COMMITTED, if your `UPDATE` hits a row that
another transaction has locked, you wait. When the other commits, Postgres
locks the row for you and re-checks your `WHERE` against the new values.
Formal record: ADR-0001 Amendment 2.

### Lesson 1 — Losing a race still leaves you holding the lock

We assumed "0 rows updated" means "we touched nothing". Wrong: if the
`UPDATE` had to wait, the row is now locked by us even though the re-check
failed, and it stays locked until our transaction ends.

```
 n1 and n2 each have 2 free GPUs. C1 and C2 are other creates that have just
 taken them (not committed yet). A ranks n1 first; B ranks n2 first
 (random tie-break).

 time  create A                     create B
  1    UPDATE n1 … waits for C1     UPDATE n2 … waits for C2
  2    C1 commits: re-check fails   C2 commits: re-check fails
       0 rows, but A HOLDS n1       0 rows, but B HOLDS n2
  3    try n2 … waits for B         try n1 … waits for A
       → each waits for the other: deadlock (40P01), one is aborted
```

Neither A nor B could even have succeeded; they deadlock on nodes they had
already lost. The verifier saw it in 5 of 40 runs.

**Fix.** Wrap each attempt in a savepoint (a "bookmark" inside the
transaction). On 0 rows, `ROLLBACK TO SAVEPOINT` undoes everything since the
bookmark, *including the lock*. Place now holds at most one node lock, as
the design always intended.

**Takeaway.** A lock taken by a failed statement is still a lock. When a
transaction tries several rows in turn, release the losers explicitly
(savepoint), or lock in a global order, or take no locks (pure snapshot).

### Lesson 2 — Check (compare-and-set) first, then decrement

The ADR said: decrement `used` and `allocated`, then do the compare-and-set
on the instance; if it fails, roll back. That looks safe ("rollback undoes
it"), but the decrement *waits* on a lock, and by the time it runs the world
has changed.

```
 tenant has 2 GPUs used, both on this instance; A and B both read
 "TERMINATING rv 5" and try → TERMINATED

 time  A                                   B
  1    UPDATE quota SET used = used - 2
       (used 2 → 0, row locked)
  2                                        UPDATE quota SET used = used - 2
                                           … waits for A
  3    CAS on instance (rv 5 → 6): wins
  4    COMMIT
  5                                        re-check: used is 0 → 0 - 2 = -2
                                           CHECK (used >= 0) fails: 23514
```

B should have been told "stale, re-read" (`ErrStaleVersion`). Instead it got
a constraint error. The CHECK stopped a double release, so no data was
wrong. But the caller got the wrong error, and a controller would treat it
as a real failure.

**Fix.** Lock the counters first (`SELECT … FOR UPDATE`, same lock order as
create), then do the compare-and-set, and decrement only if it matched:

```
 time  A                                   B
  1    lock quota, lock node
  2                                        lock quota … waits for A
  3    CAS wins → used -= 2, allocated -= 2
  4    COMMIT
  5                                        gets lock; CAS sees rv 6 → 0 rows
                                           ROLLBACK → ErrStaleVersion
```

Two related fixes from the same review: the version is checked *before*
asking the state machine "is this edge legal?", so a loser always hears
"stale" and never "illegal move"; and the release happens only when leaving
a state that holds capacity (`PENDING → FAILED` releases nothing).

**Takeaway.** "Decide, then act" must be in the same order inside the
transaction as in your head: prove you won (compare-and-set) before you
change anything that depends on winning. Constraints are the backstop, not
the control flow.

**Interview one-liners.**
- "In Postgres a waiting UPDATE that loses its re-check keeps the row lock;
  use a savepoint per attempt to release it."
- "Lock, then CAS, then mutate. Mutating before the CAS makes the loser act
  on a state it never saw."
- "Check the version before the business rule, so a loser gets a retryable
  error instead of a misleading one."

**Protects:** invariant 1 (no over-allocation; the conditional UPDATE still
decides), invariant 4 (losers get `ErrStaleVersion`), invariant 5 (released
exactly once), and the candidate invariant 7 (accounting matches).

---

## Deferred to v2

| Item | Why it waits |
|---|---|
| Queueing (PENDING + timeout) | v1 fails fast by choice (D3) |
| Separate leader-elected scheduler service | Needed only for a global queue (D4) |
| Priority classes → tenant fairness → FIFO → aging | Nothing to order without a queue; aging fixes head-of-line blocking |
| Aligned GPU-index allocation | Count-based is enough for v1 (D8) |
| Batch create, multi-node instances, MIG | Out of scope |
