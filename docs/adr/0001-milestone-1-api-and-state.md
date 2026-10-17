# ADR-0001: Milestone 1 — API server, Postgres state, placement and idempotency

Status: Accepted (tech lead, 2026-10-16)
Date: 2026-10-15
Milestone: 1

## Context

Users need to ask for a GPU instance and get a fast, honest answer: either
"accepted, GPUs reserved on node X" or a clear reason why not. Milestone 1
builds the part of the system that makes that decision and records it
durably, correctly under concurrency, with N API replicas running at once.

The reasoning behind every choice is in `docs/Discussion.md` (D1–D15). This
ADR does not re-argue those; it turns them into a buildable M1 design and
adds the few decisions M1 needs that the discussion left open.

**In scope (M1):** HTTP API (create, get, delete); Postgres schema and
migrations; the state machine rules; idempotent create; per-tenant quota;
in-request placement via the scheduler library (D4–D7); outbox rows written in
every state-change transaction; a store-level transition function; metrics.

**Out of scope, and where it lands:**

| Later work | Milestone |
|---|---|
| `GPUInstance` CRD, controller, Pods on kind nodes, boot/shutdown delays in the Pod, provisioning timeout, injected boot failures | M2 |
| Node sync loop (nodes discovered from Kubernetes), leader election (controller only, D13) | M2 |
| Outbox relay → NATS JetStream → metering consumer → usage table | M3 |
| Chaos tests, OpenTelemetry traces, dashboards | M4 |
| Queue / PENDING, scheduler service, priorities, aligned GPU indices | v2 |
| Real auth, list endpoint, idempotency-key expiry | not planned yet |

**How M1 works without a controller.**
- *Nodes:* there is no node sync yet, so node rows come from a seed command
  (`make seed-nodes N=10`) that upserts fake H100/A100 nodes with capacity 8.
  It follows D15 column ownership: it writes inventory columns and never
  touches `allocated`. In M2 the node sync loop replaces it as the writer.
- *Lifecycle:* states after SCHEDULED are driven by an internal function,
  `store.Transition`, which M1 tests call directly. In M2 the controller
  calls the very same function, so the logic is built and tested once.
  *(Amended: in M1 an instant lifecycle driver in the apiserver also calls
  `store.Transition`, so create reaches RUNNING and DELETE reaches TERMINATED
  over HTTP. See Amendment 1.)*

## Concepts (for the tech lead)

- **Transaction:** a group of writes that all commit or none do. A crash
  mid-transaction leaves no trace: Postgres rolls it back.
- **Conditional update (compare-and-set):** `UPDATE … WHERE <condition still
  true>`. If another request changed the row first, the condition fails and
  0 rows are updated, so we *know* we lost and never overwrite their work.
- **Row lock + re-check:** when two transactions update the same row,
  Postgres makes the second wait until the first commits, then re-evaluates
  the `WHERE` against the *new* row. That is why `allocated + n <= capacity`
  stays true even with 100 concurrent writers. *(Losing the re-check still
  leaves the second transaction holding that row lock until it ends; see
  Amendment 2.)*
- **Outbox:** a table we write events into in the same transaction as the
  state change, so a committed change can never lose its event. In M1 rows
  only accumulate; the relay that publishes them arrives in M3.
- **Idempotency key:** a client-chosen ID for a request. If the client
  retries (e.g. after a timeout), the server recognizes the key and returns
  the original result instead of creating a second instance.
- **resource_version (rv):** a counter bumped on every write. A writer says
  "update this if it is still at rv 3"; if it is at 4, the write is refused.
  Over HTTP the rv is the `ETag`, and a client sends it back as `If-Match`.
- **Lock ordering:** if transaction A locks X then Y while B locks Y then X,
  both wait forever (deadlock). Every transaction locks tables in the same
  order, so this cannot happen.

## Design

```
 client ──HTTP──► apiserver replica (×N)
                     │  internal/api        validate, map errors to HTTP
                     │  internal/store      one transaction per request
                     │  internal/scheduler  Place(ctx, tx, req)  (library, D4)
                     │  internal/statemachine  pure transition rules
                     ▼
                  Postgres 16: instances · nodes · tenant_quota ·
                               idempotency_keys · outbox
```

### Data model

| Table | Key columns | Constraints that guard invariants |
|---|---|---|
| `nodes` | `id` PK, `gpu_type`, `capacity`, `allocated`, `schedulable`, `failure_domain`, `last_seen` | `CHECK (allocated >= 0 AND allocated <= capacity)` |
| `tenant_quota` | PK `(tenant_id, gpu_type)`, `quota_limit` (default 16), `used` | `CHECK (used >= 0 AND used <= quota_limit)` |
| `instances` | `id` PK (`inst_…`), `tenant_id`, `gpu_type`, `gpu_count`, `node_id` FK, `state`, `resource_version`, `failure_reason`, `created_at`, `updated_at` | `CHECK gpu_count IN (1,2,4,8)`; `CHECK state IN (…7 states)`; index on `(tenant_id)` and `(node_id)` |
| `idempotency_keys` | PK `(tenant_id, key)`, `request_hash`, `instance_id`, `created_at` | PK makes a key usable once per tenant; FK to `instances` is `DEFERRABLE INITIALLY DEFERRED` |
| `outbox` | `id` bigserial (order), `event_id` uuid UNIQUE, `aggregate_id`, `event_type`, `payload` jsonb, `occurred_at`, `published_at` NULL | partial index `WHERE published_at IS NULL` for the M3 relay (id order ≠ commit order; see Amendment 2) |

The CHECK constraints are a second guard behind the conditional updates: even
a buggy query cannot commit an over-allocation. "Holds capacity" means state
in {SCHEDULED, PROVISIONING, RUNNING, TERMINATING}; capacity and quota are
released on the transition to TERMINATED or FAILED *(only from a state that
holds capacity; see Amendment 2)*.

### API (M1)

Tenant identity comes from an `X-Tenant-ID` header (a stand-in for auth).
Errors use one envelope: `{"error": {"code": "...", "message": "..."}}`.

| Request | Success | Errors |
|---|---|---|
| `POST /v1/instances` + `Idempotency-Key`, body `{gpu_type, gpu_count}` | `202` `{id, state: SCHEDULED, node_id, gpu_type, gpu_count, resource_version}` (state at commit), `ETag`; in M1 a `GET` right after shows RUNNING (Amendment 1) | `400 invalid_request` · `403 quota_exceeded` · `409 idempotency_key_reused` · `503 insufficient_capacity` + `Retry-After: 30` · also `413` / `408` / header limits (see Amendment 2) · `503 conflict_retry` on deadlock, `503` when the DB is unreachable (see Amendment 3) |
| `GET /v1/instances/{id}`, optional `If-None-Match` | `200` instance, `ETag: "<rv>"`; `304` if rv unchanged | `400` (no tenant) · `404 not_found` (also for another tenant's instance — don't leak existence) |
| `DELETE /v1/instances/{id}`, optional `If-Match` | `202` with state TERMINATING; the instance ends TERMINATED with GPUs and quota released (Amendment 1) | `404` · `409 stale_resource_version` · `409 invalid_transition` · `503` on deadlock or DB unreachable before TERMINATING commits (see Amendment 3) |

A replayed create returns `202` with the original instance id, its *current*
state, and `Idempotent-Replayed: true`. DELETE is idempotent: on an instance
already TERMINATED or FAILED it returns `202` with the current state and
writes no event; on TERMINATING it finishes the walk to TERMINATED
(Amendment 1). DELETE works from SCHEDULED, PROVISIONING and RUNNING (Q1).
Clients learn about state changes by polling GET with `If-None-Match`
(Discussion D16).

### The create transaction, step by step

Before the transaction: validate the body (400), generate the instance id,
compute `request_hash` = SHA-256 of the *parsed* body (so whitespace or field
order don't count as a different body).

```
BEGIN  (READ COMMITTED)
1. INSERT INTO idempotency_keys … ON CONFLICT DO NOTHING
     0 rows → key exists: read it, ROLLBACK
              same hash → 202 replay of that instance
              diff hash → 409 idempotency_key_reused
     (a concurrent first use makes us wait for it to commit, then we see it)
2. INSERT tenant_quota row with default 16 ON CONFLICT DO NOTHING
   UPDATE tenant_quota SET used = used + n
     WHERE tenant_id=$t AND gpu_type=$g AND used + n <= quota_limit
     0 rows → ROLLBACK → 403 quota_exceeded                     (D2)
3. scheduler.Place(ctx, tx, req)                              (D4, D5)
     snapshot: SELECT schedulable nodes of gpu_type with free ≥ n (no locks)
     rank: best fit (least free after) → spread tenant across
           failure_domain → random
     for each candidate:           (each attempt in a savepoint: Amendment 2)
       UPDATE nodes SET allocated = allocated + n
         WHERE id=$c AND schedulable AND allocated + n <= capacity
       1 row → chosen; 0 rows → next candidate
     none left → ROLLBACK → 503 insufficient_capacity + Retry-After
4. INSERT instances (state SCHEDULED, node_id, resource_version 1)
5. INSERT outbox (event_type 'instance.scheduled', new event_id)
COMMIT → 202
```

Lock order everywhere: `idempotency_keys → tenant_quota → nodes → instances
→ outbox`. A transaction holds at most one node lock (losing candidates are
not locked). *(Superseded: that is only true because each reserve attempt
runs in a savepoint; see Amendment 2.)* Postgres deadlock detection plus a
40P01 → 503 mapping is only the backstop (code and Retry-After: see
Amendment 3).

### Lifecycle transitions (store.Transition)

```go
func (s *Store) Transition(ctx context.Context, id string, to statemachine.State,
    expectedRV int64, reason string) (Instance, error)
// ErrNotFound, ErrInvalidTransition, ErrStaleVersion
```
*(Steps 1–3 below are superseded: the error order, the release condition and
"decrement, then compare-and-set" were all corrected. Use Amendment 2, item 2.)*

1. Read the instance (no lock). `statemachine.CanTransition(cur, to)` or
   `ErrInvalidTransition`.
2. BEGIN. If `to` is TERMINATED or FAILED: `used -= n` on `tenant_quota`, then
   `allocated -= n` on `nodes` (same lock order as create).
3. `UPDATE instances SET state=$to, resource_version=rv+1 WHERE id=$1 AND
   state=$cur AND resource_version=$expectedRV`; 0 rows → ROLLBACK (undoing
   step 2) → `ErrStaleVersion`.
4. INSERT outbox `instance.<state>`. COMMIT.

Retrying a transition that already happened fails at step 3 and changes
nothing, which is the idempotent behavior the M2 controller relies on (D11).
*(Still true; the retry now gets `ErrStaleVersion`, usually from the
version check on the read. See Amendment 2.)*

## Decisions

Agreed decisions applied as-is: request shape D1, quota D2, fail fast D3,
scheduler as library D4, filter/score/reserve D5, GPU-count limit D6,
bin-packing D7, count-only GPUs D8, Postgres + outbox D9, resource_version
D10, Postgres as customer truth D11, 202 at SCHEDULED D12, node column
ownership D15. New M1-only decisions:

### M1-a: How to stay correct under concurrency
| Option | Pros | Cons |
|---|---|---|
| A. READ COMMITTED + conditional updates | Only conflicting rows wait; no retries; easy to reason per statement | Must design each `WHERE` carefully |
| B. SERIALIZABLE everywhere | Database catches every anomaly | Whole-transaction retries on 40001; harder to explain failures |
| C. `SELECT … FOR UPDATE` on all candidate nodes | Simple mental model | Locks many nodes per request; replicas queue behind each other |

**Choice:** A, as D5/D10 already assume. **Protects:** invariants 1, 4, and new 6.

### M1-b: Where idempotency keys live
| Option | Pros | Cons |
|---|---|---|
| A. Separate `idempotency_keys` table, inserted first | Duplicate stops before touching quota/capacity; room for expiry later | One more table |
| B. Unique `(tenant_id, idempotency_key)` column on `instances` | Fewer tables | Duplicate is only detected at step 4, after quota and node were locked |

**Choice:** A. Only *successful* creates keep their key; a 403/503 rolls the
key back, so retrying the same key later can succeed when capacity frees up
(see Q2). **Protects:** invariant 2.

### M1-c: How to exercise lifecycle without a controller
| Option | Pros | Cons |
|---|---|---|
| A. `store.Transition` called by tests | Same code the controller will call; no throwaway parts | States beyond SCHEDULED unreachable over HTTP in M1 |
| B. Fake in-process "controller" goroutine with timers | Visible end-to-end in M1 | Throwaway; sleeps in our code, which D12 forbids |
| C. Admin HTTP endpoint to set state | Easy manual testing | An API that bypasses the real driver; must be removed later |

**Choice:** A. **Protects:** invariants 3, 4, 5.
*(Amended: "unreachable over HTTP" left DELETE unable to release capacity. M1
adds an instant lifecycle driver that calls the same `store.Transition` with
no timers or sleeps, unlike option B. See Amendment 1.)*

### M1-d: Test database
| Option | Pros | Cons |
|---|---|---|
| A. `make db-up` (one Postgres 16 container) + a fresh database per test | Fast; one command for the verifier | Docker must be running first |
| B. testcontainers-go per package | Self-contained | Slower; extra dependency |

**Choice:** A. Integration tests use the `integration` build tag and run via
`make test-integration` (`go test -race -count=1 -tags=integration ./...`
against `DATABASE_URL`); `make test` stays dependency-free.

## Invariants this design protects

| Invariant | Protected by |
|---|---|
| 1 No over-allocation | Conditional `UPDATE nodes` (step 3) + `CHECK allocated <= capacity` |
| 2 Idempotent create | `idempotency_keys` PK + request hash (step 1) |
| 3 No lost events | outbox insert inside every state-change transaction |
| 4 Optimistic concurrency | `resource_version` in `Transition` and `If-Match` on DELETE |
| 5 Safe deletion | Capacity and quota released only in the terminal transition's transaction (finalizer comes in M2) |
| **6 (candidate) Quota under concurrency** | A tenant's GPUs in capacity-holding states never exceed its `quota_limit` per GPU type, even with N replicas — conditional `UPDATE tenant_quota` + CHECK *(now enforced and tested; see Amendment 2, item 5)* |
| **7 (candidate) Accounting matches** | `nodes.allocated` = sum of `gpu_count` of capacity-holding instances on that node; `tenant_quota.used` likewise per tenant/type. Checked by tests and a drift metric (D15 backstop) *(see Amendment 2, item 5)* |

## Failure modes

| Failure | Effect | How we recover / why it's safe |
|---|---|---|
| Concurrent creates, same tenant | Both want the same `tenant_quota` row | Row lock serializes them; second re-checks `used + n <= limit` on the new value → one may get 403. Other tenants are not blocked |
| Concurrent creates racing for the last GPUs on a node | Both pick the same best-fit node from their snapshot | Second waits on the row lock, re-checks, gets 0 rows, rolls back to its savepoint (dropping that lock), tries the next candidate; if none, 503. Never over-allocates (Amendment 2) |
| Duplicate request, same key + body (client retry) | Could double-provision | Step 1 finds the key → replay, no new quota/capacity/event. If the first is still in flight, the second waits for its commit |
| Same key, different body | Client bug or key reuse | 409 `idempotency_key_reused`; nothing written |
| Crash mid-transaction (process, network, DB restart) | Partial reservation? | Not possible: the transaction rolls back as a whole. Client sees an error or timeout and retries with the same key |
| Commit succeeded but response lost | Client thinks it failed | Retry with the same key returns the same instance |
| Stale `resource_version` (DELETE `If-Match`, or `Transition`) | Would overwrite a newer state | 0 rows → 409 / `ErrStaleVersion`; caller re-reads |
| Postgres down or slow | Requests fail | 503 with no partial state; per-request statement timeout via ctx; `/readyz` fails so the load balancer stops routing (see Amendment 3) |
| Two transactions lock rows in opposite order | Deadlock | Fixed lock order prevents it; Postgres detection aborts one as backstop. Lost-race locks count too: savepoints in Place (Amendment 2). Answered `503 conflict_retry` (see Amendment 3) |
| Concurrent identical transitions (retries, N controller replicas) | Double release; `used` below 0 | Lock counters, then compare-and-set; only the winner decrements. Losers get `ErrStaleVersion` (Amendment 2) |
| Accounting drift from a bug | `allocated` ≠ real usage | Drift gauge alerts (D15: alert, never silently fix) |

## Observability

*(The list below is the original plan; what shipped, with extra create
results and a stuck-TERMINATING gauge, is in Amendment 3.)*

Metrics (`gpucp_<subsystem>_<name>_<unit>`), all on `/metrics`:
- `gpucp_api_requests_total{route,code}`, `gpucp_api_request_duration_seconds{route}`
- `gpucp_create_results_total{result}`: scheduled, replayed, quota_exceeded, insufficient_capacity, key_reused
- `gpucp_scheduler_reserve_conflicts_total`: lost a node race and fell back (shows contention between replicas)
- `gpucp_scheduler_place_duration_seconds`
- `gpucp_nodes_gpus{gpu_type,kind=capacity|allocated}`
- `gpucp_scheduler_fragmented_gpus{gpu_type}`: free GPUs on nodes with fewer than 8 free, i.e. that cannot serve an 8-GPU request (D7)
- `gpucp_store_allocation_drift_nodes`: nodes where invariant 7 fails
- `gpucp_instance_transitions_total{from,to}`
- `gpucp_outbox_unpublished_rows`: grows in M1 by design; the M3 relay drains it

Logs: `log/slog` JSON with `instance_id`, `tenant`, `node_id`, `state`,
`resource_version`, `result`. Health: `/healthz` (process up), `/readyz`
(DB reachable; see Amendment 3). Traces wait for M4.

## Stories

S1-1 to S1-16 in `docs/backlog.md` (Milestone 1; S1-14 from Amendment 1, S1-16 from Amendment 3). Test-engineer stories
S1-11 and S1-12 are written from this ADR in parallel with the implementer.

## Resolved questions (tech lead, 2026-10-16)

- **Q1. DELETE on a SCHEDULED instance:** add the `SCHEDULED → TERMINATING`
  edge (mirrors PROVISIONING → TERMINATING). DELETE works from SCHEDULED,
  PROVISIONING and RUNNING. *Revised in Amendment 1:* DELETE then continues
  to TERMINATED in M1 via the instant lifecycle driver.
- **Q2. Failed creates and the key:** 403/503 are not stored under the key;
  the `idempotency_keys` row is inserted inside the create transaction, so
  the rollback removes it and the same key can be retried later (M1-b;
  re-confirmed 2026-10-16).
- **Q3. Tenant identity:** `X-Tenant-ID` header with no auth for M1
  (re-confirmed 2026-10-16).
- Also accepted: `Retry-After: 30` on 503; make targets `db-up`, `db-down`,
  `test-integration`, `seed-nodes`.

## Amendment 1 (2026-10-16): instant lifecycle driver

Decided by the tech lead; reasoning in `docs/Discussion.md` D17.

**Problem.** With M1-c as written, nothing moved an instance to TERMINATED,
so `DELETE` never released GPUs or quota over HTTP.

**Change.** A small driver in the apiserver (`internal/lifecycle`, M1 only)
walks the legal path right away, one `store.Transition` per step. Each step
is its own transaction with one outbox row and an rv check; each step uses
the rv returned by the previous one.

```
POST   : create txn commits ─► respond 202 {state: SCHEDULED}
         driver: SCHEDULED ─► PROVISIONING ─► RUNNING
DELETE : <cur> ─► TERMINATING ─► TERMINATED   (release GPUs + quota)
         respond 202 {state: TERMINATING}
```

- The state machine is **not** changed: no shortcut edges. DELETE from
  SCHEDULED/PROVISIONING uses the existing `→ TERMINATING` edges.
- The driver runs **in the request, after commit and before the response**,
  on a context detached from client cancellation (`context.WithoutCancel` +
  a short timeout), so a test's `GET` right after the response is
  deterministic. The response body is the state the request's own transaction
  committed (SCHEDULED for create, TERMINATING for DELETE), exactly as in M2.
- No `time.Sleep`, timers or goroutines that outlive the request.
- On any error (including `ErrStaleVersion` because a concurrent DELETE won)
  the driver logs it and stops; the instance stays in its last committed
  state. A create-driver step losing to a DELETE is expected and harmless.
- DELETE without `If-Match` re-reads and retries a stale rv a bounded number
  of times (3) before `409`; with a stale `If-Match` it returns `409` at once.
- Final state stays TERMINATED; rows are kept for billing history.

**Accepted weakness.** A crash between the create commit and the driver's
steps leaves the instance in SCHEDULED or PROVISIONING; a crash mid-delete
leaves it in TERMINATING. Nothing repairs it automatically in M1. A user can
still DELETE it (all three states accept DELETE), which releases capacity.
The M2 controller's level-triggered reconcile fixes it properly, and the
driver is deleted in M2.

**Unchanged for clients in M2.** POST → `202` SCHEDULED, DELETE → `202`
TERMINATING, poll GET for RUNNING / TERMINATED. Only the time between states
grows. The outbox history is identical in M1 and M2.

**Protects:** invariant 3 (one outbox row per step), invariant 4 (every step
carries the rv), invariant 5 (release only in the transaction that reaches
TERMINATED). Story: S1-14.

## Amendment 2 (2026-10-16): corrections found in review

Verifier reviews of S1-5, S1-7/S1-8 and S1-9 (plus a note from S1-2) found
places where the accepted text above is wrong or incomplete. The code
already follows this amendment; this records it. Plain-language lessons
and timelines are in `docs/Discussion.md` D18. Story: A-4.

### 1. Place: a lost race keeps its row lock (S1-5)

**Was:** "A transaction holds at most one node lock (losing candidates are
not locked)."

**Wrong because:** under READ COMMITTED, when our `UPDATE nodes … WHERE
allocated + n <= capacity` has to wait for another transaction's lock on
that row, Postgres locks the row for us once the other one commits, *then*
re-checks the `WHERE`. If the re-check fails (0 rows), we **keep the lock**
until our transaction ends. So Place could end up holding the lost node plus
the node it finally reserved. Two creates each holding a lost node the other
wants then deadlock: the verifier saw `40P01` in 5 of 40 runs.

**Now:** each reserve attempt runs in its own savepoint (`internal/scheduler`
`reserve`):

```
SAVEPOINT
UPDATE nodes … WHERE id=$c AND schedulable AND allocated + n <= capacity
  1 row  → RELEASE SAVEPOINT (keeps the update and its lock) → chosen
  0 rows → ROLLBACK TO SAVEPOINT (drops the lock it waited for) → next candidate
```

When Place returns, the transaction holds a lock on the reserved node only.
Cost: 3 round trips per attempt instead of 1 (watch
`gpucp_scheduler_place_duration_seconds`, S1-13). Evidence:
`TestPlaceReleasesLockOnLostRace` (fails 10/10 without the savepoint).
**Protects:** the lock-order rule, and so availability; invariant 1 is
unchanged (the conditional UPDATE still decides).

### 2. store.Transition: check order, lock-then-CAS, release condition (S1-9)

Replaces steps 1–3 of §Lifecycle transitions.

**Error order:** `ErrNotFound` → `ErrStaleVersion` (rv mismatch) →
`ErrInvalidTransition`. The version is checked *before* the state machine,
so a caller that lost a race always hears "stale, re-read", never a
misleading "illegal move" computed from a state it never saw. The M1 driver
and the DELETE retry loop (S1-14, S1-10, not built yet) depend on this. A retry of a step that
was already applied returns `ErrStaleVersion` and changes nothing, so D11
(idempotent transitions) still holds.

**Release condition:** release only when `HoldsCapacity(from) &&
IsTerminal(to)`. "`to` is terminal" alone is wrong: v2's PENDING → FAILED
never reserved anything and must release nothing
(`TestTransitionFromPendingDoesNotRelease`).

**Transaction order:**

```
read instance (no lock): not found? rv ≠ expected? edge illegal?   → errors above
BEGIN
  if release: SELECT … FROM tenant_quota … FOR UPDATE    (lock order unchanged:
              SELECT … FROM nodes … FOR UPDATE            quota → nodes → instances → outbox)
  UPDATE instances SET state=$to, rv=rv+1
    WHERE id=$1 AND state=$from AND rv=$expected RETURNING *
    0 rows → ROLLBACK → ErrStaleVersion        (nothing was decremented)
  if release: used -= n; allocated -= n        (only the winner gets here)
  INSERT outbox instance.<state>
COMMIT
```

The rows to lock come from the unlocked read; that is safe because
`tenant_id`, `gpu_type`, `gpu_count` and `node_id` never change while an
instance holds capacity, and the code refuses to release if the row returned
by the UPDATE differs from what it locked.

**Why the old order failed:** it decremented first, then did the CAS. A
loser waiting on the quota row lock resumed after the winner committed and
subtracted from a row that was already released, so `used` went below 0 and
the CHECK constraint aborted it with `23514` instead of `ErrStaleVersion`.
The CHECK prevented a double release, but the caller got the wrong error.
Evidence: `TestTransitionConcurrentRepeatReleasesOnce` (100 racers, exactly
1 winner, 1 outbox row, released once), `TestCreateTransitionChurnNoDeadlock`
(fails on any `40P01`; totals back to 0).
**Protects:** invariants 4 and 5, and 7.

### 3. API errors (S1-7, S1-8)

Adds to §API (M1):

| Case | Response |
|---|---|
| Create body larger than 4 KiB | `413 request_too_large` |
| Server read deadline hit while reading the body | `408 request_timeout` |
| Malformed JSON, unknown field, trailing data | `400 invalid_request`, fixed message `malformed JSON body` |
| `X-Tenant-ID` longer than 64, `Idempotency-Key` longer than 255, or either one not visible ASCII (0x21–0x7E, so no spaces) | `400 invalid_request`. The header value is never echoed back |
| Same key, different body | `409 idempotency_key_reused` |
| Same key, same body | `202`, original instance in its current state, header `Idempotent-Replayed: true` |
| Any unexpected error | `500 internal`, message `internal error` |

Rule: messages are fixed or built only from validated fields. The underlying
error (decoder output can contain socket addresses; database errors contain
SQL details) is logged and never returned. Header limits exist because both
values go into primary keys, and an oversized btree entry would be a 500,
not a 400.

### 4. M3 note: outbox order (S1-2)

`outbox.id` comes from a sequence, which is assigned at INSERT time, not at
COMMIT. Transaction A can take id 10, B take id 11, and B commit first. A
relay that remembers "last id seen = 11" would skip 10 forever (a lost
event, breaking invariant 3). The M3 relay must scan
`WHERE published_at IS NULL` (the partial index exists for this) and mark
rows published; never use a "last id seen" cursor. Consumers still dedupe
by `event_id`.

### 5. Invariants 6 and 7: from candidate to enforced

| Invariant | Enforced by | Tested by (today) | Still missing |
|---|---|---|---|
| 6 Quota under concurrency | Conditional `UPDATE tenant_quota … used + n <= quota_limit` + CHECK | `TestCreateQuotaExceeded` (fails if the quota `WHERE` is dropped), `TestQuotaIsPerGPUType`, `TestSchemaRejectsOverQuota`, churn test (S1-6, S1-7, S1-9) | A many-requests-one-tenant race at the limit (S1-11) |
| 7 Accounting matches | Reserve and release happen in the same transaction as the state change; release only for the CAS winner | `TestTransitionFullLifecycle`, `TestTransitionConcurrentRepeatReleasesOnce`, `TestCreateTransitionChurnNoDeadlock` (totals return to their start) (S1-9) | Direct `allocated = SUM(gpu_count)` check (S1-11) and the drift gauge (S1-13) |

**Proposal (tech lead to decide):** promote 6 and 7 into the invariant list
in `CLAUDE.md`, so the verifier checks them on every story. This ADR does
not edit `CLAUDE.md`.

## Amendment 3 (2026-10-17): observability and error mapping (S1-13, S1-16)

S1-13 shipped a few things this ADR did not say, and its review found two
error cases that disagreed with §Failure modes. This records what the code
does (`internal/api`, `internal/metrics`). Story: S1-16.

### 1. Deadlock (SQLSTATE 40P01) → `503 conflict_retry` + `Retry-After: 1`

Postgres breaks a deadlock by aborting one of the two transactions. That
transaction rolled back as a whole (for a create, its idempotency key too),
so retrying with the same key is safe. `store.IsDeadlock` detects it anywhere
in the error chain; the API answers:

| Case | Response |
|---|---|
| Create aborted by 40P01 | `503 conflict_retry`, `Retry-After: 1` |
| DELETE: read or walk aborted by 40P01 *before* TERMINATING committed | `503 conflict_retry`, `Retry-After: 1` |
| DELETE: TERMINATING committed, release step aborted by 40P01 (or any other real error) | `202` with state TERMINATING. The delete was accepted; another DELETE resumes the walk (Amendment 1) |

- **Why 1s, not 30s like capacity:** a deadlock is a clash between two
  specific transactions, and one is already gone; an immediate retry almost
  always succeeds. 30s is for "wait until GPUs free up", which is not the
  problem here.
- **Why a separate code:** `insufficient_capacity` tells a client "the fleet
  is full". Reusing it would make dashboards and clients misread a lock bug
  as a capacity shortage.
- **Logged at Error:** with the fixed lock order a deadlock should never
  happen, so each one is a bug to investigate. The SQL error is logged, never
  returned.
- **Order matters in DELETE:** the "TERMINATING already committed → 202"
  branches are checked before the deadlock case, so an accepted delete is
  never reported as failed.

### 2. Database unreachable → 503

Create and DELETE with Postgres unreachable (connection refused, pool closed)
answer **503 unavailable with `Retry-After: 5` (Postgres restarts take seconds: 1s would hammer it, 30s would keep clients away too long)** instead of `500 internal`, as §Failure modes
already promised. Nothing was written, so a retry is safe. DELETE follows the
same rule as above: once TERMINATING has committed it is 202, not 503.

### 3. `/healthz` vs `/readyz`

| Endpoint | Checks | Fails when | Used by |
|---|---|---|---|
| `/healthz` | nothing beyond "process serves HTTP" | process hung or dead | liveness: restart the process |
| `/readyz` | DB ping, 1s timeout | DB unreachable or slow, or no check configured → `503 not_ready` | readiness: stop routing traffic here |

`/healthz` deliberately ignores Postgres: if a database outage failed
liveness, every replica would be restarted, which cannot fix the database
and adds a restart storm on top. Neither endpoint (nor `/metrics`) is counted
in `gpucp_api_requests_total`.

### 4. Metrics actually shipped

All on a **private registry** (not Prometheus' global one, so each test reads
exact values), plus the standard Go and process collectors.

Event metrics (incremented by code):
- `gpucp_api_requests_total{route,code}`, `gpucp_api_request_duration_seconds{route}`.
  `route` is the mux pattern (e.g. `GET /v1/instances/{id}`), never the raw
  path; requests that match no pattern (404/405) get `route="unmatched"`, so
  random paths cannot create unbounded series.
- `gpucp_create_results_total{result}`: the original five (`scheduled`,
  `replayed`, `quota_exceeded`, `insufficient_capacity`, `key_reused`) plus
  `invalid` (any 4xx before the store), `conflict_retry` (40P01; should stay
  0) and `error` (500, and 503 `unavailable` when the DB is unreachable). Every create lands in exactly one bucket; all series
  start at 0.
- `gpucp_scheduler_reserve_conflicts_total`, `gpucp_scheduler_place_duration_seconds`.
- `gpucp_instance_transitions_total{from,to}`: counted only after the
  transition commits; a lost CAS adds 0 (pinned by an S1-16 test).

Database gauges, computed **at scrape time** by read-only queries with a 2s
timeout. If the DB is down they are left out of that scrape (and logged);
the event metrics are still served:
- `gpucp_nodes_gpus{gpu_type,kind=capacity|allocated}`.
- `gpucp_scheduler_fragmented_gpus{gpu_type}`: sum of `free mod 8` over
  *schedulable* nodes of that type, i.e. free GPUs that cannot serve an
  8-GPU request (D7). Per type, because free H100s and A100s never combine.
- `gpucp_store_allocation_drift_nodes`: nodes where invariant 7 fails.
  Reported, never fixed (D15): "fixing" it would hide the bug that caused it.
- `gpucp_outbox_unpublished_rows`.
- **New:** `gpucp_store_stuck_terminating_instances`: instances in
  TERMINATING for more than 1 minute. In M1 a delete finishes within its
  request (at most 3s), so these were left by a crash or a failed release
  step and still hold GPUs and quota until someone DELETEs again. This is
  the signal S1-10 chose instead of a "re-DELETE" hint in the response.
  **Revisit in M2:** real shutdown delays in the Pod will make 1 minute
  wrong.

### 5. Known gap: `/metrics` on the API port

`/metrics` is served on the API port with no in-flight limit. Each scrape
runs the DB queries above, so a burst of scrapes competes with API traffic
for pool connections. Acceptable for M1 (one Prometheus, local fleet).
**Revisit in M2:** separate port and/or `promhttp` `MaxRequestsInFlight`.
