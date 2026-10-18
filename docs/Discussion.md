# Design discussion — v1

Record of the design discussion between the tech lead and the architect
(2026-10-15; D17 and D18 added 2026-10-16; Milestone 2 decisions D19–D36 added 2026-10-17 at the end) that produced the v1 decisions. For each decision: the question
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

---

# Design discussion — Milestone 2 (controller)

Decisions the tech lead accepted on 2026-10-17 for Milestone 2 (D19 onward,
numbered M2-1 … M2-16 in the meeting). Same format as above. ADR-0002
(`docs/adr/0002-milestone-2-controller.md`) turns them into a buildable design.

## Summary (M2)

| # | Meeting | Area | Decision |
|---|---|---|---|
| D19 | M2-1 | GPUInstance CRD | One CR per instance, named from the instance ID; Postgres stays truth, CR status mirrors DB (DB wins); CR gives kubectl visibility, Pod ownership (GC) and the finalizer |
| D20 | M2-2 | How the controller learns about DB changes | Poll the `instances` table (not the outbox): every 1s by `updated_at` with 5s overlap on DB time; every 30s all non-terminal ids; enqueue ids only |
| D21 | M2-3 | Write order across two stores | Kubernetes objects first, then the DB transition; every step idempotent |
| D22 | M2-4 | Leader election | Lease, 3 replicas (M2b) |
| D23 | M2-5 | Node sync | Kubernetes Nodes → DB `nodes`; never writes `allocated` (basic sync in M2a per D39; health policy M2b) |
| D24 | M2-6 | k8s/DB drift | Detect orphan Pods, missing Pods, CR ≠ DB; metrics + repair toward the DB (M2b) |
| D25 | M2-7 | Failure paths | Boot failure / provisioning timeout → FAILED; Pod dies while RUNNING → FAILED (until M2c); capacity always released |
| D26 | M2-8 | Postgres in kind | Helm chart in the cluster; migrations as a Job |
| D27 | M2-9 | Milestone split | M2a lifecycle (1 replica), M2b HA + nodes + drift, M2c auto-recovery |
| D28 | M2-10 | Who moves which state | API: create → SCHEDULED, DELETE → TERMINATING. Controller: everything else (incl. TTL → TERMINATING) |
| D29 | M2-11 | New edge | RUNNING → FAILED |
| D30 | M2-12 | TTL | `ttl_seconds` → `expires_at`; RequeueAfter until expiry, then TERMINATING (M2a) |
| D31 | M2-13 | Event sources | Informers + DB poller feed **one** instance workqueue |
| D32 | M2-14 | Node health | READY / NOT_READY / PROBATION / CORDONED / QUARANTINED + `status_changed_at` (M2b) |
| D33 | M2-15 | Auto-recovery | RECOVERING state, fence old Pod, keep quota, re-place; no capacity → retry 10 min then FAILED (M2c) |
| D34 | M2-16 | Queue and framework | kubebuilder + controller-runtime; rate-limited workqueue; InstanceReconciler (8 workers) + NodeReconciler (2) |
| D35 | — | Fake GPU Pod | Init container sleeps `BOOT_SECONDS`, fails at `BOOT_FAIL_RATE`; `preStop` sleep; no `time.Sleep` in Go |
| D36 | — | M1 leftovers | Remove the M1 instant driver in M2a; S1-17 follow-ups ride in M2b |
| D37 | ADR-0002 Q1 | Kubernetes names | `inst_x` → `inst-x` for CR and Pod names; original ID in a label |
| D38 | ADR-0002 Q3 | Terminal CRs | Controller deletes the CR once the row is TERMINATED or FAILED; Postgres keeps history |
| D39 | ADR-0002 Q2 | Node sync timing | Basic node sync moves **into M2a** (READY / NOT_READY / CORDONED); probation, quarantine, flapping stay in M2b |
| D40 | S2-5 review | Queue key → instance id | Stored generated column `instances.k8s_name = replace(id,'_','-')`, UNIQUE + indexed; Reconcile looks rows up by it |
| D41 | S2-19 flake | Node with never-known capacity | Stays NOT_READY until capacity is reported, whatever the event order (S2-20) |

---

## D19 — GPUInstance CRD (M2-1)

**Question.** Postgres already holds every instance. Why also create a
Kubernetes object per instance, and which one wins if they disagree?

**Discussion.** A CRD (Custom Resource Definition) teaches Kubernetes a new
object type, here `GPUInstance`. Options:

| Option | Pros | Cons |
|---|---|---|
| A. No CRD; controller creates Pods straight from DB rows | One store fewer to keep in sync | Nothing to hang a finalizer or owner on; `kubectl get` shows anonymous Pods; deleting a Pod by hand loses the link |
| B. **CRD as a mirror** (DB is truth) | `kubectl get gpuinstances` shows state; Pod has an owner (garbage collection); a finalizer can block deletion until capacity is released | Two copies of state; must define "DB wins" |
| C. CRD as the truth (DB only for billing) | The "pure Kubernetes" way | Throws away M1's transactions, quota, idempotency and outbox; etcd is the wrong store for billing history (D9) |

**Decision.** B. One CR per instance, named from the instance ID. The CR
does three jobs:
1. **Visibility:** `kubectl get gpuinstances` shows state, node and age.
2. **Ownership:** the Pod has an `ownerReference` to the CR, so deleting the CR
   lets Kubernetes' garbage collector delete the Pod.
3. **Finalizer:** a finalizer is a tag on an object that says "do not really
   delete me until controller X removes this tag". Our controller removes it
   only after the DB row is TERMINATED or FAILED, i.e. after capacity was
   released.

The CR's `status` is copied *from* the DB. If they disagree, the DB wins and
the CR is overwritten. Nobody reads the CR to decide customer state.

**Protects:** invariant 5 (finalizer), D11 (one source of truth).

---

## D20 — DB poller: how the controller learns about changes (M2-2)

**Question.** Kubernetes tells the controller when a Pod or CR changes. But a
new instance only exists as a row in Postgres. How does the controller hear
about it?

**Options.**
- **A. Read the outbox.** Every change already writes an outbox row. But the
  outbox is a *history* ("it went A → B → C"), meant for the M3 relay and
  metering. A controller doesn't care about history; it cares about *now*.
  Using the outbox would also mean tracking "which rows have I processed",
  a second consumer cursor with its own bugs (see Amendment 2 §4: id order ≠
  commit order).
- **B. Poll the `instances` table.** Ask "which rows changed recently?" and
  reconcile those. Level-triggered: if we miss one poll, the next poll or the
  resync catches it.
- **C. Postgres LISTEN/NOTIFY.** Lowest latency, but notifications are lost
  if the controller is disconnected, so you still need B as a backstop.

**Decision.** B, with C as a possible later optimization.
```sql
-- every 1s
SELECT id, now() AS db_now FROM instances
WHERE updated_at > $last_db_now - interval '5 seconds';
-- every 30s (full resync)
SELECT id FROM instances WHERE state NOT IN ('TERMINATED','FAILED');
```
- **Only ids are enqueued.** The reconcile loads the full row itself, so it
  always acts on the freshest data, never on a copy taken by the poller.
- **DB time, not controller time.** `$last_db_now` is the `now()` the previous
  poll got from Postgres. If the controller's clock is off by 3 minutes,
  nothing breaks.
- **Why the 5s overlap?** `updated_at = now()` is set to the time the
  *transaction started*, not when it committed. A transaction that started at
  10:00:00.9 and committed at 10:00:01.2 has `updated_at` 10:00:00.9; a poll
  that ran at 10:00:01.0 didn't see it (not committed), and the next poll asks
  for "> 10:00:01.0". Without overlap, that row is missed until the 30s resync.
  Re-seeing a row costs nothing because…
- **…the workqueue dedupes.** If id X is already waiting in the queue, adding
  it again is a no-op. If X is being reconciled right now, it is marked
  "dirty" and runs exactly once more after the current run. If X is waiting
  on a timer (RequeueAfter), the add makes it run now; the timer still fires
  later and causes one extra, harmless reconcile.
- **An unchanged reconcile writes nothing.** That is what makes re-seeing
  rows cheap: polls never cause writes or outbox rows by themselves.
- Needs an index on `updated_at` (the column already exists; the index does not).

**Protects:** D11 (level-triggered), invariant 3 (the poller writes nothing,
so it cannot create outbox rows).

---

## D21 — Kubernetes first, then the DB (M2-3)

**Question.** A step touches two stores (e.g. "create Pod" and "mark
PROVISIONING"). There is no transaction across both. Which goes first?

**Discussion.** Look at the crash in the middle:

| Order | Crash between the two writes leaves | Next reconcile |
|---|---|---|
| **K8s first, then DB** | Pod exists, DB still SCHEDULED | Sees SCHEDULED, "create Pod" → already exists, fine; then transitions. Converges |
| DB first, then K8s | DB says PROVISIONING, no Pod | Must recognize "PROVISIONING but no Pod" as "create it" rather than "Pod lost". Ambiguous, and customer state was ahead of reality |

For the release direction it matters even more: if the DB said TERMINATED
(GPUs released, maybe already given to someone else) while the old Pod was
still running, two instances would share the GPUs.

**Decision.** Do the Kubernetes action first, then record it in the DB via
`store.Transition`. Every step is idempotent: "create Pod" treats
`AlreadyExists` as success, "delete Pod" treats `NotFound` as success, and a
repeated transition gets `ErrStaleVersion` and changes nothing.
Rule of thumb: **customer state may lag reality, never lead it.**

**Protects:** invariants 1 and 5 (capacity is released only after the Pod is
confirmed gone).

---

## D22 — Leader election (M2-4, M2b)

**Decision.** 3 controller replicas, one active, via a Kubernetes Lease
(controller-runtime's built-in leader election). Same reasoning as D13: the
Lease gives efficiency (one replica does the work, others are warm
standbys), and D10 conditional updates give correctness when a paused old
leader wakes up. Lands in M2b; M2a runs 1 replica.

---

## D23 — Node sync (M2-5; basic sync moved to M2a by D39)

**Decision.** A NodeReconciler watches Kubernetes Nodes and upserts the
inventory columns of `nodes` (type from the label, capacity from the
extended resource, failure domain, status). It **never writes `allocated`**
(D15 column ownership). It replaces `make seed-nodes` for real nodes; seed
stays for DB-only scale tests.

*Update (D39): the basic sync (upsert + READY / NOT_READY / CORDONED) is
built in M2a; M2b adds the health policy of D32.*

**Protects:** invariants 1 and 7.

---

## D24 — Drift detection between Kubernetes and the DB (M2-6, M2b)

**Question.** What if the two stores disagree?

| Drift | Example cause | Repair (always toward the DB) |
|---|---|---|
| Orphan Pod | Pod exists, DB row is TERMINATED/FAILED or missing | Delete the Pod (it is using GPUs nobody pays for) |
| Missing Pod | DB says RUNNING, no Pod | FAILED (M2a/b) or recover (M2c), D25 |
| CR ≠ DB | CR status says RUNNING, DB says TERMINATING | Overwrite CR status from DB |

**Decision.** Each kind of drift gets a counter/gauge, and the controller
repairs only in the *safe direction*: make Kubernetes match the DB, never
the reverse. `nodes.allocated` drift (invariant 7) stays report-only, as in
D15: "fixing" a counter hides the bug that broke it.

---

## D25 — Failure paths (M2-7)

**Decision.**
- **Boot failure** (init container exits non-zero) → delete Pod → when gone,
  PROVISIONING → FAILED, reason `boot_failed`.
- **Provisioning timeout** (5 min, measured on DB time from when the row
  entered PROVISIONING) → same, reason `provisioning_timeout`.
- **Pod dies while RUNNING** → RUNNING → FAILED, reason `pod_lost` (new edge,
  D29). M2c replaces this with recovery (D33).
- Every path ends in a transaction that releases GPUs and quota, and only
  after the Pod is confirmed gone (D21).
- Before declaring a Pod "missing" the controller re-checks with a live API
  read, not only the cache: a cache that is behind must never fail a healthy
  instance.

**Protects:** invariant 5.

---

## D26 — Postgres in kind (M2-8)

**Options.** A. Keep Postgres in Docker on the laptop, reach it from kind via
the host. B. **Postgres inside kind via a Helm chart.** C. A Postgres operator
(CloudNativePG).

**Decision.** B: one Postgres 16 Pod with a PVC, deployed by Helm. Migrations
run as a Kubernetes **Job** (a Pod that runs to completion) before the
apiserver and controller start. A is fragile (host networking differs per
OS); C is a big new thing to learn for no M2 benefit. `make db-up` stays for
`make test-integration`.

---

## D27 — Split M2 into M2a / M2b / M2c (M2-9)

**Discussion.** All of M2 would be ~40 stories. Splitting keeps each part
shippable and reviewable, and follows the story-planning order "single
replica first, then HA".

| Part | Delivers |
|---|---|
| **M2a** | kind + Postgres in kind, CRD, controller (1 replica), DB poller, full lifecycle incl. delete, failures, TTL; basic node sync (D39); M1 driver removed; Helm + `make deploy` |
| **M2b** | Leader election (3 replicas), node health policy (probation, quarantine, flapping), drift detection and repair, S1-17 follow-ups |
| **M2c** | Auto-recovery (RECOVERING state) |

---

## D28 — Who drives which transition (M2-10)

**Question.** With a real controller, should DELETE still walk to TERMINATED?

**Decision.** No. Each transition has exactly one owner:

| Transition | Owner |
|---|---|
| (create) → SCHEDULED | API |
| SCHEDULED / PROVISIONING / RUNNING → TERMINATING on DELETE | API |
| … → TERMINATING on TTL expiry | controller |
| SCHEDULED → PROVISIONING → RUNNING, TERMINATING → TERMINATED, any → FAILED, (M2c) RECOVERING | controller |

All of them go through `store.Transition` (one transaction + one outbox row +
`resource_version` compare-and-set). The DELETE response is unchanged
(`202` TERMINATING); clients poll until TERMINATED. Two owners can still race
on the same row (API DELETE vs controller → RUNNING): the compare-and-set
picks one, the loser re-reads.

**Protects:** invariants 3 and 4.

---

## D29 — RUNNING → FAILED (M2-11)

**Question.** (S1-1 review) Without this edge, a node dying under a RUNNING
instance can only be recorded as TERMINATING → TERMINATED, which looks like
the user deleted it.

**Decision.** Add RUNNING → FAILED. FAILED is terminal and releases
capacity like any other terminal transition (`HoldsCapacity(from) &&
IsTerminal(to)`). Metering (M3) can then tell "user stopped it" from "we lost
it" (useful for refunds and SLOs). `CLAUDE.md`'s diagram must be updated
(orchestrator/tech lead).

---

## D30 — TTL (M2-12)

**Decision.** Optional `ttl_seconds` in the create body; stored as
`expires_at = created_at + ttl` (DB time). The reconciler returns
`RequeueAfter(expires_at − db_now)` so the queue wakes it at expiry (no
sleeps, survives restarts because the deadline lives in the DB). At expiry
the controller transitions to TERMINATING, reason `ttl_expired`, and the
normal delete path finishes. `ttl_seconds` is part of the idempotency hash
(same key + different TTL → 409).

---

## D31 — One instance queue fed by informers and the poller (M2-13)

**Question.** Changes arrive from two worlds: Kubernetes (Pod became Ready)
and Postgres (DELETE committed). Two queues or one?

**Decision.** One. Both sources turn their event into the same key, the
instance ID, and add it to the same queue. The queue guarantees **one worker
per key at a time**, so two reconciles of the same instance can never run in
parallel inside one process. With two queues that guarantee would be lost.

---

## D32 — Node status (M2-14, M2b; READY / NOT_READY / CORDONED in M2a per D39)

**Decision.** Replace the `schedulable` boolean with
`status ∈ {READY, NOT_READY, PROBATION, CORDONED, QUARANTINED}` plus
`status_changed_at`. The scheduler places only on READY. A node that comes
back from NOT_READY goes to PROBATION for a while before READY (don't trust
it at once); a node that flaps (repeatedly goes NOT_READY) goes to
QUARANTINED until an operator clears it. CORDONED mirrors `kubectl cordon`.

**Protects:** invariant 1 and availability (no placement on bad nodes).

---

## D33 — Auto-recovery (M2-15, M2c)

**Question.** When a RUNNING instance's Pod or node dies, should we fail it
or bring it back elsewhere?

**Decision.** Bring it back, by default:
- New state **RECOVERING** (RUNNING → RECOVERING → PROVISIONING …).
- **Fence first:** force-delete the old Pod so it can never come back and
  share GPUs with the replacement.
- **Keep quota, release the old node:** the tenant still "owns" its GPUs
  (quota held), but the old node's `allocated` is released and the same
  scheduler (`Place`) picks a new node.
- This splits invariant 7: RECOVERING **holds quota but not a node**. The
  state machine gets `HoldsQuota(state)` and `HoldsNode(state)` instead of one
  `HoldsCapacity`.
- **Grace deadline** before declaring a Pod lost, computed on DB time, waited
  out with RequeueAfter.
- **No capacity:** stay RECOVERING (quota kept), retry every 30s via
  RequeueAfter, up to 10 minutes, then FAILED reason `no_capacity` and
  release quota.
- `max_recoveries` per instance; `recovery_policy` default `auto`,
  per-instance `none` to opt out (then D25: FAILED).
- `store.Recover` is internal only; no recovery API.

---

## D34 — Controller framework and work queue (M2-16)

**Question.** Write our own loop, or use controller-runtime? How many
workers, and how do retries work?

**Decision.**
- **kubebuilder scaffold + controller-runtime**: the standard way to write
  controllers; gives informers, cache, workqueue, leader election, metrics.
- **Rate-limited workqueue** (client-go): each failing key retries with
  exponential backoff capped at ~5 min, plus an overall token bucket so a
  storm of failures can't hammer Postgres. `Forget` on success resets a key's
  backoff. Timers (TTL, timeouts, grace) use `RequeueAfter`, which is not
  counted as a failure.
- **Two controllers, each with its own queue:**
  - **InstanceReconciler, 8 workers.** Handles create, run, delete, TTL,
    timeouts and (M2c) recovery. Recovery is **not** a separate worker pool:
    "one worker per key" only holds within one queue, so a second queue
    could reconcile the same instance twice at once. Reconciles are short
    (they never wait; they RequeueAfter). If starvation ever appears, a
    priority queue is the fix, not a second queue.
  - **NodeReconciler, 2 workers.**
- **Shared informers:** Node (feeds the node queue, and the instance queue
  via a cache index on the CR's `spec.nodeName`, so a node going NotReady
  wakes its instances), Pod (label `app=gpu-instance`, mapped by the
  `instance-id` label), GPUInstance.
- **DB poller → instance queue** via `source.Channel`.
- **Sizing:** DB pool 16 connections (more than the 8 workers, so poller and
  metrics never wait behind them); Kubernetes client QPS 50, burst 100;
  worker counts are flags.
- Workqueue metrics (depth, latency, retries) exposed on `/metrics`.

---

## D35 — Fake GPU Pod

**Decision.** Per D12/D14: an init container sleeps `BOOT_SECONDS`
(random 10–30) and exits non-zero with probability `BOOT_FAIL_RATE`; the main
container runs `sleep infinity` and requests `gpu.example.com/gpu: N`, pinned
with `spec.nodeName`; a `preStop` hook sleeps a few seconds so TERMINATING
is visible. All delays live in the Pod; our Go code never calls `time.Sleep`.

---

## D36 — M1 leftovers

**Decision.**
- The M1 instant driver (`internal/lifecycle`, D17) is deleted in M2a, once
  the controller drives the lifecycle. API tests change from "GET shows
  RUNNING right after create" to "SCHEDULED"; tests that need later states
  call `store.Transition` directly. `CLAUDE.md`'s M1 note is updated by the
  orchestrator/tech lead.
- The S1-17 follow-ups go to M2b: fail fast with 503 during a burst while the
  DB is down, `cmd/seed-nodes` connect timeout, warn at startup if
  `connect_timeout ≥ 10s`, parallelize the black-hole subtests.

---

## D37 — Kubernetes names for instances (ADR-0002 Q1)

**Question.** Instance IDs look like `inst_ab3k…`. Kubernetes object names
must be lowercase letters, digits, `-` and `.` only, so `_` is rejected. How
do we name the CR and Pod?

**Options.** A. Map `_` → `-` in one function (`k8sName`), keep the real ID
in the label `gpu.example.com/instance-id` (label values may contain `_`).
B. Change the ID format to `inst-…`, which changes the API contract and
leaves existing rows with the old form.

**Decision.** A (tech lead, 2026-10-17). The mapping is one-way and used only
for Kubernetes names; everything that looks an instance up (event handlers,
reconcile key) uses the label or the CR's `spec.instanceID`, never reverses
the name. The mapping is safe because our IDs contain exactly one `_` and the
random part is base32 (no `-`), so two IDs can never map to the same name.

---

## D38 — Delete the CR when the instance is finished (ADR-0002 Q3)

**Question.** After TERMINATED or FAILED, keep the GPUInstance CR or delete it?

**Discussion.** Keeping them makes `kubectl get gpuinstances` grow forever and
puts history in etcd, which D9 says is the wrong store for it. The DB row
(and its outbox events) already holds the full history.

**Decision.** Delete (tech lead, 2026-10-17). Once the DB row is terminal the
controller deletes the CR and removes its finalizer. The finalizer still
guarantees the order: capacity released in the DB first, CR gone second.
History is read from Postgres (`GET /v1/instances/{id}`).

**Protects:** invariant 5.

---

## D39 — Basic node sync moves into M2a (ADR-0002 Q2)

**Question.** The scheduler places on DB `nodes` rows, and the controller pins
Pods to those names, so the rows must match the kind workers. The proposal
was a seed Job with explicit names until M2b. The tech lead asked instead to
bring a real, small node sync into M2a.

**Why it is better.** A seed list is a second copy of the cluster layout that
silently goes stale (add a worker, forget the list). Node sync discovers the
inventory, as D15 always intended, and M2b only adds health policy on top.

**Decision (M2a scope).**
- **NodeReconciler**, its own queue, 2 workers, sharing the Node informer.
  Only Nodes with the label `gpu.example.com/type` are GPU nodes; the
  control-plane node is ignored.
- **Upsert** `nodes` by node name: `gpu_type` (label), `capacity`
  (`status.capacity["gpu.example.com/gpu"]`), `failure_domain`
  (`topology.kubernetes.io/zone`), `last_seen`, `status`,
  `status_changed_at`. **Never `allocated`** (D15). The SQL for the upsert
  never mentions `allocated`, so it keeps its default 0 on insert and is
  untouched on update.
- **Status** (M2a uses three of the five values in D32):

  | Kubernetes says | `status` |
  |---|---|
  | Node object gone, or Ready condition not True | NOT_READY |
  | Ready, but `spec.unschedulable` (cordoned) | CORDONED |
  | Ready and schedulable | READY |

  `status_changed_at` changes only when `status` changes. PROBATION,
  QUARANTINED and flapping detection are M2b. The CHECK constraint accepts
  all five now, so M2b needs no constraint change.
- **Scheduler uses `status = 'READY'`** both in its snapshot filter and in
  the reserve `UPDATE … WHERE status = 'READY' AND allocated + n <= capacity`.
  Options were: (A) keep the `schedulable` boolean and have sync derive it,
  or (B) replace it with `status`. **B**: two columns that must agree is a
  drift bug waiting to happen. The migration adds `status` (backfilled from
  `schedulable`) and drops `schedulable`; seed-nodes and metrics switch too.
  Because the check is in the reserve's `WHERE`, a node going NOT_READY at
  the same moment as a placement is safe: the row lock orders the two, and a
  placement that commits first is handled like any instance on a dying node
  (its Pod fails → FAILED, capacity released).
- **A node that disappears** (Node deleted from Kubernetes): the row is
  **kept** and marked NOT_READY, never deleted (D15: rows are referenced by
  instances and billing). Instances on it are handled by the instance path:
  their Pods are gone, so RUNNING → FAILED `pod_lost` (D25). The Node
  informer's delete event enqueues the node name; the reconcile gets
  NotFound and marks the row. Deletes missed while the controller was down
  are caught by a sweep after cache sync and every 5 min: any row whose name
  is not a GPU Node in the cache → NOT_READY. (DB-only rows from
  `make seed-nodes` are for controller-less scale tests and are not used in
  kind; if run together, the sweep marks them NOT_READY, which is correct.)
- **Capacity lowered below `allocated`:** the update would violate
  `CHECK allocated <= capacity`. Sync writes capacity with a guard
  (`… WHERE allocated <= $new_capacity`); if that refuses, the old capacity
  stays, everything else is still synced, and
  `gpucp_nodes_capacity_conflicts` reports it with a log line. Report,
  don't clobber; same rule as D15's drift gauge.
- **`gpu_type` is set on insert only.** If the label changes later, sync
  keeps the stored type and reports `gpucp_nodes_type_conflicts`: changing
  the type under running H100 instances would corrupt quota accounting.
- Node changes do **not** yet wake that node's instances (CR `spec.nodeName`
  index); that stays in M2b with the rest of node health.
- `make seed-nodes` stays for local, DB-only scale tests; it is not used in
  kind.

**Protects:** invariant 1 (placement only on READY nodes, guarded in the
reserve's `WHERE`; capacity never set below `allocated`), invariant 7 (sync
never writes `allocated`).

## D40 — Finding an instance from its queue key (S2-5 review)

**Question.** The poller and the informers queue `{gpucp, K8sName(id)}`
(D37), so a worker gets `inst-x`, not the DB id `inst_x`. When the CR exists
the id is on its label, but a brand-new instance found by the poller has no
CR yet. How does Reconcile find its row? D37 says the mapping is never
reversed, because two ids could map to one name.

**Options.**
- **A. Stored generated column** `k8s_name text GENERATED ALWAYS AS
  (replace(id,'_','-')) STORED UNIQUE`; lookup `WHERE k8s_name = $1`.
- B. Expression lookup `WHERE replace(id,'_','-') = $1` with an expression
  index. No new column, but two colliding ids are not caught, so a lookup
  could match two rows.
- C. Change the id format to be Kubernetes-safe (`inst-…`). No mapping at
  all, but it changes the M1 API and tests and reverses D37.

**Decision: A** (tech lead, 2026-10-17). Added as migration 0008 in S2-7.

**Why.** The UNIQUE constraint makes a name collision impossible at insert
time, so the lookup can never be ambiguous. It is indexed, so it is one fast
query, and Postgres computes the column itself, so no code can write a wrong
value. Cost: one more column and migration.

## D41 — A node whose GPU capacity was never reported (S2-19 follow-up)

**Question.** A Node can be Ready before its kubelet reports `gpu.example.com/gpu`. Depending on which
event the controller saw first, the row ended READY/0 or NOT_READY/0. Which is right?

**Options.** A: a row whose capacity has never been known stays NOT_READY until capacity is reported.
B: drop the special case; status simply follows Ready (READY/0 for a few seconds).

**Decision: A** (tech lead, 2026-10-18), story S2-20.

**Why.** A controller must be level-triggered: the same observed state gives the same row, whatever
the event history. With A, READY always means "can take work", the READY node metric is honest, and
`status_changed_at` is not stamped before the node is usable (which the M2b probation clock relies on).
Safety was never at risk: placement needs `allocated + n <= capacity`, so nothing fits into 0.

