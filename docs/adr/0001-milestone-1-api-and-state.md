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

## Concepts (for the tech lead)

- **Transaction:** a group of writes that all commit or none do. A crash
  mid-transaction leaves no trace: Postgres rolls it back.
- **Conditional update (compare-and-set):** `UPDATE … WHERE <condition still
  true>`. If another request changed the row first, the condition fails and
  0 rows are updated, so we *know* we lost and never overwrite their work.
- **Row lock + re-check:** when two transactions update the same row,
  Postgres makes the second wait until the first commits, then re-evaluates
  the `WHERE` against the *new* row. That is why `allocated + n <= capacity`
  stays true even with 100 concurrent writers.
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
| `outbox` | `id` bigserial (order), `event_id` uuid UNIQUE, `aggregate_id`, `event_type`, `payload` jsonb, `occurred_at`, `published_at` NULL | partial index `WHERE published_at IS NULL` for the M3 relay |

The CHECK constraints are a second guard behind the conditional updates: even
a buggy query cannot commit an over-allocation. "Holds capacity" means state
in {SCHEDULED, PROVISIONING, RUNNING, TERMINATING}; capacity and quota are
released on the transition to TERMINATED or FAILED.

### API (M1)

Tenant identity comes from an `X-Tenant-ID` header (a stand-in for auth).
Errors use one envelope: `{"error": {"code": "...", "message": "..."}}`.

| Request | Success | Errors |
|---|---|---|
| `POST /v1/instances` + `Idempotency-Key`, body `{gpu_type, gpu_count}` | `202` `{id, state: SCHEDULED, node_id, gpu_type, gpu_count, resource_version}`, `ETag` | `400 invalid_request` · `403 quota_exceeded` · `409 idempotency_key_reused` · `503 insufficient_capacity` + `Retry-After: 30` |
| `GET /v1/instances/{id}`, optional `If-None-Match` | `200` instance, `ETag: "<rv>"`; `304` if rv unchanged | `400` (no tenant) · `404 not_found` (also for another tenant's instance — don't leak existence) |
| `DELETE /v1/instances/{id}`, optional `If-Match` | `202` with state TERMINATING | `404` · `409 stale_resource_version` · `409 invalid_transition` |

A replayed create returns `202` with the original instance id, its *current*
state, and `Idempotent-Replayed: true`. DELETE is idempotent: on an instance
already TERMINATING, TERMINATED or FAILED it returns `202` with the current
state and writes no event. DELETE on a SCHEDULED instance moves it to
TERMINATING (Q1). Clients learn about state changes by polling GET with
`If-None-Match` (Discussion D16).

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
     for each candidate:
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
not locked). Postgres deadlock detection plus a 40P01 → 503 mapping is only the
backstop.

### Lifecycle transitions (store.Transition)

```go
func (s *Store) Transition(ctx context.Context, id string, to statemachine.State,
    expectedRV int64, reason string) (Instance, error)
// ErrNotFound, ErrInvalidTransition, ErrStaleVersion
```
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
| **6 (candidate) Quota under concurrency** | A tenant's GPUs in capacity-holding states never exceed its `quota_limit` per GPU type, even with N replicas — conditional `UPDATE tenant_quota` + CHECK |
| **7 (candidate) Accounting matches** | `nodes.allocated` = sum of `gpu_count` of capacity-holding instances on that node; `tenant_quota.used` likewise per tenant/type. Checked by tests and a drift metric (D15 backstop) |

## Failure modes

| Failure | Effect | How we recover / why it's safe |
|---|---|---|
| Concurrent creates, same tenant | Both want the same `tenant_quota` row | Row lock serializes them; second re-checks `used + n <= limit` on the new value → one may get 403. Other tenants are not blocked |
| Concurrent creates racing for the last GPUs on a node | Both pick the same best-fit node from their snapshot | Second waits on the row lock, re-checks, gets 0 rows, tries the next candidate; if none, 503. Never over-allocates |
| Duplicate request, same key + body (client retry) | Could double-provision | Step 1 finds the key → replay, no new quota/capacity/event. If the first is still in flight, the second waits for its commit |
| Same key, different body | Client bug or key reuse | 409 `idempotency_key_reused`; nothing written |
| Crash mid-transaction (process, network, DB restart) | Partial reservation? | Not possible: the transaction rolls back as a whole. Client sees an error or timeout and retries with the same key |
| Commit succeeded but response lost | Client thinks it failed | Retry with the same key returns the same instance |
| Stale `resource_version` (DELETE `If-Match`, or `Transition`) | Would overwrite a newer state | 0 rows → 409 / `ErrStaleVersion`; caller re-reads |
| Postgres down or slow | Requests fail | 503 with no partial state; per-request statement timeout via ctx; `/readyz` fails so the load balancer stops routing |
| Two transactions lock rows in opposite order | Deadlock | Fixed lock order prevents it; Postgres detection aborts one as backstop |
| Accounting drift from a bug | `allocated` ≠ real usage | Drift gauge alerts (D15: alert, never silently fix) |

## Observability

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
(DB reachable). Traces wait for M4.

## Stories

S1-1 to S1-13 in `docs/backlog.md` (Milestone 1). Test-engineer stories
S1-11 and S1-12 are written from this ADR in parallel with the implementer.

## Resolved questions (tech lead, 2026-10-16)

- **Q1. DELETE on a SCHEDULED instance:** add the `SCHEDULED → TERMINATING`
  edge (mirrors PROVISIONING → TERMINATING). DELETE works from SCHEDULED,
  PROVISIONING and RUNNING.
- **Q2. Failed creates and the key:** 403/503 are not stored under the key;
  the same key can be retried later (M1-b).
- **Q3. Tenant identity:** `X-Tenant-ID` header with no auth for M1.
- Also accepted: `Retry-After: 30` on 503; make targets `db-up`, `db-down`,
  `test-integration`, `seed-nodes`.
