# Design discussion — v1

Record of the design discussion between the tech lead and the architect
(2026-10-15) that produced the v1 decisions. For each decision: the question
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
| D12 | State machine | Unchanged; v1 instances start at SCHEDULED |
| D13 | Leader election | Controller only |
| D14 | Fake GPUs | kind workers + label + extended resource `gpu.example.com/gpu: 8`; instance = Pod |
| D15 | Node inventory | Rows inserted once via Node sync; placement only updates `allocated`; single owner per column; drift reconciler |

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

## D12 — State machine in v1

**Decision.** Unchanged:
```
PENDING → SCHEDULED → PROVISIONING → RUNNING → TERMINATING → TERMINATED
   └──────────┴────────────┴──► FAILED
```
Because placement happens in the request (D3/D4), v1 instances are created
directly as **SCHEDULED**. PENDING stays in the model for the v2 queue, so
nothing is redesigned later.

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

## Deferred to v2

| Item | Why it waits |
|---|---|
| Queueing (PENDING + timeout) | v1 fails fast by choice (D3) |
| Separate leader-elected scheduler service | Needed only for a global queue (D4) |
| Priority classes → tenant fairness → FIFO → aging | Nothing to order without a queue; aging fixes head-of-line blocking |
| Aligned GPU-index allocation | Count-based is enough for v1 (D8) |
| Batch create, multi-node instances, MIG | Out of scope |
