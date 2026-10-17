# ADR-0002: Milestone 2 — GPUInstance controller on kind

Status: Accepted (tech lead, 2026-10-17)
Date: 2026-10-17
Milestone: 2 (split into M2a, M2b, M2c)

## Context

After M1, an instance reaches RUNNING only because an instant driver inside
the apiserver walks the states (D17). Nothing actually runs, nothing can
fail during boot, and a crash mid-walk leaves an instance stuck forever.

M2 adds the **controller**: a separate process that watches Postgres and
Kubernetes and drives each instance through its real lifecycle, with a Pod
on a fake GPU node standing in for the machine. Reasoning for every choice
is in `docs/Discussion.md` D19–D36 (accepted by the tech lead, 2026-10-17).
This ADR does not re-argue them; it makes them buildable.

**Scope by part:**

| Part | In scope |
|---|---|
| **M2a** (this ADR's stories) | kind cluster with 4 fake GPU workers; Postgres in kind (Helm) + migrate Job; `GPUInstance` CRD; controller, **1 replica**; DB poller; lifecycle SCHEDULED → PROVISIONING → RUNNING; delete path with finalizer; boot failure, provisioning timeout, Pod lost while RUNNING (new RUNNING → FAILED edge); TTL; **basic node sync** (NodeReconciler, `nodes.status` READY / NOT_READY / CORDONED, D39); M1 driver removed; workqueue tuning + metrics; Helm chart + `make deploy` + smoke test |
| **M2b** (epic) | Leader election, 3 replicas; node health policy (PROBATION, QUARANTINED, flapping) and Node events waking that node's instances; k8s/DB drift detection and repair; S1-17 follow-ups |
| **M2c** (epic) | Auto-recovery: RECOVERING state, fencing, re-placement |

**Out of scope:** outbox relay, NATS, metering (M3); Prometheus/Grafana,
chaos suite (M4); queue / PENDING (v2).

## Concepts (for the tech lead)

- **Controller / reconcile loop:** a function `Reconcile(id)` that reads
  "what should be" (the DB row) and "what is" (Pod, CR), and takes *one* step
  to close the gap. It is called again and again; it must be safe to call any
  number of times (**idempotent**).
- **Level-triggered vs edge-triggered:** edge-triggered reacts to "X changed
  from A to B" and breaks if it misses an event. Level-triggered looks at the
  current state every time, so a missed event is fixed by the next look. All
  our loops are level-triggered.
- **CRD / CR:** a Custom Resource Definition adds a new object type to
  Kubernetes (`GPUInstance`); a CR is one object of that type.
- **Finalizer:** a string on an object that blocks its real deletion until
  the controller removes it. Lets us say "release capacity before this goes".
- **ownerReference:** "this Pod belongs to that CR". When the CR is deleted,
  Kubernetes' garbage collector deletes the Pod.
- **Informer:** a client that lists all objects of a type once, then watches
  for changes, keeping a local copy (cache) up to date.
- **Workqueue:** a queue of keys (instance IDs) with dedupe, retries with
  backoff, and "one worker per key at a time".
- **RequeueAfter:** "call me again with this key in N seconds". How the
  controller waits (for timeouts, TTL) without sleeping.
- **Lease:** a Kubernetes object used for leader election (M2b).

## Under the hood: how controller-runtime turns events into Reconcile calls

This is what happens between "a Pod became Ready" and our `Reconcile` running.

```
                 kube-apiserver                         Postgres
                      │ ▲                                  │
      1. LIST, then   │ │ writes (Create Pod,              │ 6. poller every 1s / 30s
         WATCH        ▼ │ patch CR status)                 ▼
 ┌──────────────── Manager (one per process) ─────────────────────────────┐
 │  shared informers: Node · Pod(app=gpu-instance) · GPUInstance           │
 │        │ 2. each event updates…                                         │
 │        ▼                                                                │
 │  in-memory cache (indexed; e.g. CR by spec.nodeName)  ◄── client.Get    │
 │        │ 3. …then calls the event handler                    reads here │
 │        ▼                                                                │
 │  handler: object → key  (Pod label instance-id → "inst_x";             │
 │                          Node → node name; poller id → key)             │
 │        │ 4. Add(key)                                                    │
 │        ▼                                                                │
 │  rate-limited workqueue  (dedupe · dirty set · backoff · delayed adds)  │
 │        │ 5. Get() by one of N workers (never two workers for one key)   │
 │        ▼                                                                │
 │  worker → Reconcile(ctx, key) → result:                                 │
 │           ok → Forget(key) · error → AddRateLimited(key)                │
 │           RequeueAfter(d) → AddAfter(key, d)                            │
 └─────────────────────────────────────────────────────────────────────────┘
```

1. **Manager.** `ctrl.NewManager` creates one shared cache and client for the
   whole process; every controller registered on it shares them, so there is
   one watch per object type, not one per controller.
2. **Informers: List, then Watch.** On start each informer does a full LIST
   (gets every Pod with our label, every Node, every CR, plus a
   `resourceVersion`), then a WATCH from that `resourceVersion` so it receives
   every later change. If the watch breaks it resumes from the last version,
   or re-LISTs if that version is too old. This is the same
   `resource_version` idea we use in Postgres (D9).
3. **Cache.** Informers keep the objects in memory. `client.Get` and
   `client.List` in our Reconcile read from this cache (fast, no API call).
   The cache may be slightly *behind* the API server; that is fine for a
   level-triggered loop, *except* before declaring something missing (D25:
   re-check with a live read).
4. **WaitForCacheSync.** Workers do not start until every informer's first
   LIST has landed in the cache. **Why:** with an empty cache, Reconcile would
   see "no Pod for inst_x" for every instance and try to create thousands of
   Pods that already exist (a storm of `AlreadyExists`), or worse, decide
   every RUNNING instance lost its Pod and FAIL all of them. Waiting for the
   sync means "no Pod in cache" really means "no Pod (as of a moment ago)".
5. **Event handler → key.** Each event is mapped to the key(s) to reconcile:
   a Pod event → its `instance-id` label; a CR event → its instance ID; a Node
   event → the node name in the node queue (M2a) and, from M2b, also the
   instance IDs of every CR whose `spec.nodeName` is that node (cache index);
   a poller id → itself. Only keys are queued, never objects:
   the reconcile reads the current state when it runs.
6. **Rate-limited workqueue.**
   - *Dedupe:* adding a key already waiting is a no-op.
   - *Dirty set:* adding a key that a worker is processing marks it dirty; it
     is re-queued once when that worker finishes. So events during a
     reconcile are never lost, and one key never runs on two workers at once.
   - *Delayed adds:* `RequeueAfter` puts the key in a timer heap; an earlier
     plain `Add` makes it run now (the timer later fires once more, harmless).
   - *Rate limiting:* `AddRateLimited` after an error waits per-key
     exponential backoff (5 ms doubling, capped ~5 min) and an overall token
     bucket; `Forget` on success resets that key's backoff.
7. **Workers.** `MaxConcurrentReconciles` goroutines loop `Get → Reconcile →
   Done`. Ours: 8 for instances, 2 for nodes (flags).
8. **The DB poller** is just another event source: a goroutine on a ticker
   that sends ids into a channel wired with `source.Channel`. It never touches
   the cache or writes anything.

## Design

```
 client ──► apiserver ×3 ──► Postgres (in kind)  ◄──────── poller (1s/30s, ids only)
            create→SCHEDULED        ▲                               │
            DELETE→TERMINATING      │ store.Transition              ▼
                                    │ (1 tx + outbox + rv CAS)  instance workqueue ◄── informers
                                    │                               │              (Pod, CR, Node)
                                    └──────── InstanceReconciler ×8 workers
                                                   │ 1. k8s first: CR (+finalizer), Pod
                                                   │ 2. then DB transition
                                                   ▼
                                    kube-apiserver ──► Pod inst-x on gpu-cp-workerN
                                                       (init: sleep BOOT_SECONDS / fail)
```

### GPUInstance CRD (`api/v1alpha1`, group `gpu.example.com`)

| Field | Meaning |
|---|---|
| `metadata.name` | `k8sName(instanceID)`: the instance ID with `_` → `-` (`inst_ab` → `inst-ab`), because Kubernetes names may not contain `_` (D37). One function, used everywhere; never reversed (lookups use the label or `spec.instanceID`) |
| `metadata.labels.gpu.example.com/instance-id` | the original instance ID (`_` is legal in label values) |
| `metadata.finalizers` | `gpu.example.com/release-capacity` |
| `spec` | `instanceID`, `tenantID`, `gpuType`, `gpuCount`, `nodeName` (copied once from the DB at creation; never changes in M2a) |
| `status` | `state`, `resourceVersion` (DB rv), `reason`, `podName`, `lastSyncTime`; copied *from* the DB, DB wins |
| printer columns | STATE, NODE, GPUS, AGE |

The Pod has the same name, labels `app=gpu-instance` and
`gpu.example.com/instance-id`, and an `ownerReference` to the CR.

### Data model changes (M2a)

| Change | Why |
|---|---|
| index `instances (updated_at)` | the 1s poller query (D20) |
| partial index `instances (id) WHERE state NOT IN ('TERMINATED','FAILED')` | the 30s resync |
| `ttl_seconds integer NULL`, `expires_at timestamptz NULL` | TTL (D30) |
| state machine: `RUNNING → FAILED` edge (no schema change; state list unchanged) | D29 |
| `nodes.status text NOT NULL` with `CHECK (status IN ('READY','NOT_READY','PROBATION','CORDONED','QUARANTINED'))`, `nodes.status_changed_at`; backfilled from `schedulable`, which is then **dropped** | D39: one column decides placement; M2a writes only READY / NOT_READY / CORDONED, M2b needs no constraint change |

Provisioning timeout uses `updated_at` of the row while in PROVISIONING
(only transitions update the row). All deadlines are computed against
`now()` from Postgres, returned with the row.

### Reconcile, by DB state (InstanceReconciler)

Every branch: load the row (+ `db_now`); if a step's write fails, return the
error (rate-limited retry). Each step is idempotent; if nothing needs doing,
nothing is written.

| DB state | Kubernetes action first | Then DB | Wait |
|---|---|---|---|
| SCHEDULED | ensure CR (+finalizer); ensure Pod (`AlreadyExists` = ok) | → PROVISIONING | — |
| PROVISIONING | Pod Ready → nothing; init failed or `now ≥ updated_at + 5m` → delete Pod | Ready → RUNNING; Pod confirmed gone after failure → FAILED (`boot_failed` / `provisioning_timeout`) | RequeueAfter(deadline) |
| RUNNING | Pod missing (confirmed by live read) or Failed → delete Pod | Pod gone → FAILED `pod_lost` (M2c: RECOVERING) | if `expires_at`: RequeueAfter(expiry) |
| SCHEDULED/PROVISIONING/RUNNING with `expires_at ≤ db_now` | — | → TERMINATING `ttl_expired` | — |
| TERMINATING | delete Pod (preStop runs; `NotFound` = ok) | Pod confirmed gone → TERMINATED (releases GPUs + quota) | Pod watch wakes us |
| TERMINATED / FAILED | ensure Pod gone; delete CR; remove finalizer | — | — |
| row not found | CR exists → log + `gpucp_controller_orphans_total` (repair in M2b) | — | — |

After each committed transition the CR status is patched from the returned
row. A **CR deleted by hand** (`kubectl delete gpuinstance`): the finalizer
holds it; Reconcile moves the DB row to TERMINATING if it is still live, and
the normal path runs; the finalizer is removed only once the row is terminal.

### Node sync (NodeReconciler, M2a scope, D39)

Own queue, 2 workers, key = node name, fed by the shared Node informer. Only
Nodes labeled `gpu.example.com/type` count.

| Kubernetes | DB `nodes` row |
|---|---|
| GPU Node exists | upsert: `gpu_type` (insert only), `capacity` (guarded), `failure_domain` (zone label), `last_seen`, `status`, `status_changed_at` (only when `status` changes). `allocated` never appears in the SQL |
| Ready condition True, schedulable | `READY` |
| Ready, `spec.unschedulable` (cordoned) | `CORDONED` |
| Ready condition not True | `NOT_READY` |
| Node deleted (event → NotFound) or missing in the sweep | row kept, `NOT_READY`; never deleted (D15). Instances on it fail through the Pod path (`pod_lost`) |
| new capacity < `allocated` | capacity left unchanged, `gpucp_nodes_capacity_conflicts` + log; other columns still synced |
| `gpu.example.com/type` label changed | stored type kept, `gpucp_nodes_type_conflicts` + log |

**Sweep:** after cache sync and every 5 min, rows whose name is not a GPU Node
in the cache are set NOT_READY (catches deletes missed while the controller
was down; it runs only after WaitForCacheSync, so an empty cache can never
mark the whole fleet NOT_READY).

**Scheduler:** the snapshot filters `status = 'READY'`, and the reserve is
`UPDATE nodes SET allocated = allocated + n WHERE id = $c AND status = 'READY'
AND allocated + n <= capacity`. The `WHERE` re-check under the row lock is
what makes a concurrent "node goes NOT_READY" safe (same pattern as D5).

### Who drives which transition (D28)

API: create → SCHEDULED; DELETE → TERMINATING (and 202 with the current state
if already TERMINATING/TERMINATED/FAILED). Controller: everything else. All
through `store.Transition`.

## Decisions

All D19–D36 are accepted. Recorded here are the three with the biggest
consequences, plus one new detail this ADR had to choose.

### M2-a: How the controller learns about DB changes (D20)
| Option | Pros | Cons |
|---|---|---|
| A. Poll `instances` by `updated_at` + 30s resync | Level-triggered; no cursor to lose; simple SQL | Up to ~1s latency; constant small query load |
| B. Consume the outbox | Already exists | It is history for M3, needs a cursor; id order ≠ commit order (ADR-0001 Amendment 2 §4) |
| C. LISTEN/NOTIFY | Lowest latency | Lost while disconnected; still needs A |
**Choice:** A (C later as an optimization). **Protects:** D11, invariant 3.

### M2-b: Write order across Kubernetes and Postgres (D21)
| Option | Pros | Cons |
|---|---|---|
| A. Kubernetes first, then DB | Crash leaves DB *behind* reality; next reconcile converges; capacity never released while a Pod lives | Customer state lags a little |
| B. DB first, then Kubernetes | Customer state updates sooner | Crash leaves DB *ahead* (says TERMINATED while Pod still runs on released GPUs) |
| C. Two-phase commit | "Atomic" | Not available across etcd and Postgres |
**Choice:** A. **Protects:** invariants 1, 5.

### M2-c: Instance queues (D31, D34)
| Option | Pros | Cons |
|---|---|---|
| A. One instance queue, 8 workers, fed by informers + poller | One worker per key holds everywhere | A slow reconcile type can delay others (mitigated: reconciles never wait) |
| B. Separate queue/workers for recovery or timeouts | Isolation | Two workers could reconcile one instance at once |
**Choice:** A; a priority queue if starvation appears. **Protects:** invariant 4 (fewer self-inflicted conflicts).

### M2-d: Kubernetes object names (new)
| Option | Pros | Cons |
|---|---|---|
| A. `k8sName(id)` = replace `_` with `-`; original ID in a label | No change to M1 IDs or API | Two spellings of one ID |
| B. Change the ID format to `inst-…` | One spelling | Changes the API contract and existing rows |
**Choice:** A (tech lead, 2026-10-17; D37). Safe because IDs have exactly one `_` and a base32 random part with no `-`, so the mapping cannot collide.

### M2-e: How placement learns a node is unusable (new, D39)
| Option | Pros | Cons |
|---|---|---|
| A. Keep `schedulable` boolean; sync derives it from status | No scheduler change | Two columns that must agree; drift between them is a new bug class |
| B. Replace it with `status`; scheduler requires `READY` | One source; M2b states drop in without a schema change | Migration + touches scheduler, seed-nodes, metrics |
**Choice:** B. **Protects:** invariant 1.

## Failure modes

| Failure | Effect | How we recover / why it's safe |
|---|---|---|
| Controller crashes mid-step (after Pod create, before → PROVISIONING) | DB says SCHEDULED, Pod exists | On restart: LIST, cache sync, resync enqueues all non-terminal ids; create Pod → `AlreadyExists`; transition proceeds |
| Controller crashes after DB → TERMINATED, before CR delete | CR with finalizer lingers | Next reconcile sees terminal row → deletes CR, removes finalizer |
| Controller starts with an empty cache | Would see "no Pods" everywhere | WaitForCacheSync before workers start; live read before declaring a Pod missing |
| Controller down for minutes | No progress; instances stay in their state; timeouts not enforced | Deadlines are on DB time, so on restart overdue instances fail at once; API keeps serving |
| Stale cache (Pod Ready not yet seen) | One reconcile does nothing | The Pod event re-enqueues the key when it lands |
| Poller misses a row (long tx, clock skew) | Delay | 5s overlap on DB time; 30s full resync; CR/Pod events |
| API DELETE races controller → RUNNING | Both CAS the same rv | One wins; the loser gets `ErrStaleVersion` and re-reads (queue retry) |
| Pod create rejected by kubelet (`OutOfgpu.example.com/gpu`) | Accounting drift between DB and node | Pod Failed → FAILED `boot_failed` path; drift gauge (M2b repair) |
| Boot failure / timeout | Instance never RUNNING | Delete Pod, then FAILED and release (D25) |
| Pod or node dies while RUNNING | GPUs gone | FAILED `pod_lost` after live-read confirmation; capacity released only once Pod is gone (M2c: recover) |
| Postgres down | Reconciles error | Rate-limited backoff (≤ 5 min per key, token bucket overall); no Kubernetes deletes are based on DB errors |
| Kubernetes API slow/down | Writes fail | Same backoff; DB unchanged because Kubernetes goes first |
| Someone deletes the CR by hand | Pod would be GC'd while billed | Finalizer: DB → TERMINATING, Pod gone, TERMINATED, then finalizer removed |
| Someone deletes the Pod by hand while RUNNING | Instance lost | `pod_lost` → FAILED (M2a), recovery (M2c) |
| Two controllers running (M2a misconfig; M2b paused leader) | Duplicate work | Every DB write is an rv CAS; every Kubernetes write is idempotent; node upserts are idempotent and never touch `allocated` |
| Node goes NotReady or is cordoned | New placements would land on it | Sync sets NOT_READY / CORDONED; reserve `WHERE status = 'READY'` refuses it even with a stale snapshot |
| Node deleted from Kubernetes | Row references disappear | Row kept as NOT_READY (event or sweep); its Pods are gone → `pod_lost`, capacity released |
| Node capacity reported lower than `allocated` | Update would break `CHECK allocated <= capacity` | Guarded update keeps old capacity; conflict metric; never clobbers |
| Controller restarts with an empty Node cache | Sweep could mark every node NOT_READY | Sweep runs only after WaitForCacheSync |

## Invariants impact

| Invariant | M2 impact |
|---|---|
| 1 No over-allocation | Unchanged guard (conditional UPDATE), now also `status = 'READY'`. Node sync never sets capacity below `allocated`. Kubelet's extended-resource check is a second, independent guard; a rejection exposes drift |
| 2 Idempotent create | `ttl_seconds` joins the request hash |
| 3 No lost events | Controller writes only via `store.Transition`; poller and status patches write no outbox rows |
| 4 Optimistic concurrency | API and controller now both write the same rows; every write is a CAS |
| 5 Safe deletion | Now enforced by the finalizer + "Pod confirmed gone before TERMINATED/FAILED" |
| 6 Quota under concurrency | Unchanged |
| 7 Accounting matches | Unchanged in M2a; node sync never writes `allocated` (D15 column ownership). M2c splits "holds capacity" into `HoldsQuota` / `HoldsNode` for RECOVERING |

## Observability (M2a)

- controller-runtime built-ins: `controller_runtime_reconcile_total{result}`,
  `controller_runtime_reconcile_time_seconds`, `workqueue_depth`,
  `workqueue_queue_duration_seconds`, `workqueue_retries_total`,
  `rest_client_requests_total`.
- `gpucp_controller_poller_ids_total{kind=changed|resync}`,
  `gpucp_controller_poller_errors_total`, `gpucp_controller_poller_lag_seconds`.
- `gpucp_controller_transitions_total{from,to,reason}`,
  `gpucp_controller_orphans_total{kind}`.
- `gpucp_nodes{gpu_type,status}`, `gpucp_nodes_capacity_conflicts`,
  `gpucp_nodes_type_conflicts`, `gpucp_controller_node_sync_total{result}`.
- Metrics on the controller's own port (`:8081`), not shared with an API.
- `gpucp_store_stuck_terminating_instances` threshold raised from 1 min to
  5 min (`preStop` + grace period), per ADR-0001 Amendment 3 "revisit in M2".
- Logs: `instance_id`, `state`, `rv`, `pod`, `node`, `step`, `result`.

## Stories

M2a: S2-1 … S2-19 in `docs/backlog.md` (S2-17 … S2-19 are node sync). M2b
and M2c are listed there as epics and will be split after M2a lands.

## Resolved questions (tech lead, 2026-10-17)

- **Q1. Kubernetes names:** map `inst_x` → `inst-x`, original ID in a label
  (M2-d A; Discussion D37).
- **Q2. Node rows in kind:** no seed Job. Basic node sync moves into M2a
  (NodeReconciler, READY / NOT_READY / CORDONED); health policy stays in M2b
  (§Node sync, M2-e; Discussion D39). `make seed-nodes` stays for local
  DB-only scale tests.
- **Q3. Terminal CRs:** the controller deletes the CR once the row is
  TERMINATED or FAILED; Postgres keeps history (Discussion D38).

- **Q4. DB-only scale-test rows vs the sweep:** accepted as proposed — no
  `source` column now; optional item in the M2b epic if we ever mix them.

## Open questions

None.
