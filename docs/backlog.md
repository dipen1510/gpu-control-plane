# Backlog

Statuses: Proposed → Approved → In progress → In review → Done
(see the `story-planning` skill). Only the tech lead approves; only a verifier
APPROVE moves a story to Done.

## Sprint 0 — Team setup  ✅

| ID | Pts | Story | Owner | Status |
|---|---|---|---|---|
| S0-1 | 1 | Repo skeleton + CLAUDE.md | orchestrator | Done |
| S0-2 | 2 | Six agent definitions with propose/execute protocol | orchestrator | Done |
| S0-3 | 2 | Skills: story-planning, design-doc | orchestrator | Done |
| S0-4 | 1 | Skills: go-conventions, deploy-kind | orchestrator | Done |
| S0-5 | 1 | This backlog | orchestrator | Done |

## Milestone 1 — API + Postgres + state machine + idempotency

### A-1 (1 pt) — System design walkthrough
Owner: architect        Reviewer: tech lead        Status: Done
Result: docs/Discussion.md (15 v1 decisions + v2 deferrals)
Why: the tech lead should understand the whole system before any code exists.
Acceptance:
  - architect explains components, request flow, state machine, data model,
    and where concurrency / consistency get hard, in plain language
  - tech lead's questions are answered
  - no files changed outside docs/
Out of scope: Milestone 1 stories (A-2)

### A-2 (2 pts) — Milestone 1 ADR and story breakdown
Owner: architect        Reviewer: tech lead        Status: Done
Result: docs/adr/0001-milestone-1-api-and-state.md (Proposed) + stories S1-1 … S1-13 below
Depends on: A-1
Acceptance:
  - docs/adr/0001-milestone-1-api-and-state.md exists with status Proposed,
    following the design-doc skill
  - Milestone 1 stories (S1-*) appended here, each 1–2 pts, each with
    verifiable acceptance criteria
Out of scope: writing any Go code

### Milestone 1 stories (from ADR-0001)

Summary (details below). Integration tests need `make db-up` first.

| ID | Pts | Story | Owner | Depends on | Status |
|---|---|---|---|---|---|
| S1-1 | 1 | State machine rules | implementer | — | Proposed |
| S1-2 | 2 | Schema, migrations and test database | implementer | — | Proposed |
| S1-3 | 1 | Seed fake nodes | implementer | S1-2 | Proposed |
| S1-4 | 1 | Node ranking: filter, best fit, tie-break | implementer | — | Proposed |
| S1-5 | 2 | Place: atomic reserve with fallback | implementer | S1-2, S1-4 | Proposed |
| S1-6 | 2 | Create (happy path) and get instance | implementer | S1-1, S1-5 | Proposed |
| S1-7 | 1 | Create fails cleanly: quota 403, capacity 503 | implementer | S1-6 | Proposed |
| S1-8 | 2 | Idempotent create | implementer | S1-6 | Proposed |
| S1-9 | 2 | Lifecycle transitions with resource_version and release | implementer | S1-1, S1-6 | Proposed |
| S1-10 | 1 | DELETE moves instance to TERMINATING | implementer | S1-9 | Proposed |
| S1-11 | 2 | Concurrency test: no over-allocation, no over-quota | test-engineer | S1-2 | Proposed |
| S1-12 | 2 | Spec tests: idempotency races and state machine | test-engineer | S1-2 | Proposed |
| S1-13 | 1 | M1 metrics and health endpoints | implementer | S1-7, S1-9 | Proposed |

S1-1, S1-2 and S1-4 can start in parallel. S1-11 and S1-12 are written from
the ADR in parallel with S1-5…S1-10 and are expected to fail until those land.

### S1-1 (1 pt) — State machine rules
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: one place decides which transitions are legal, so the API and the M2 controller can never disagree
Design link: ADR-0001 §Lifecycle transitions; CLAUDE.md §Instance state machine
Depends on: —
Acceptance:
  - `internal/statemachine` defines the 7 states and `CanTransition(from, to State) bool`,
    `IsTerminal(State) bool`, `HoldsCapacity(State) bool`; no I/O, no DB imports
  - `TestCanTransition` is table-driven over all 49 (from, to) pairs and matches the
    CLAUDE.md diagram exactly (includes PROVISIONING → TERMINATING; no backward edges)
  - `TestTerminalStatesNeverChange`: no edge leaves TERMINATED or FAILED
  - `make test` and `make lint` pass
Out of scope: persistence (S1-9); SCHEDULED → TERMINATING edge (ADR-0001 Q1)

### S1-2 (2 pts) — Schema, migrations and test database
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: the CHECK constraints are the last line of defense for invariants 1 and 6, even against buggy queries
Design link: ADR-0001 §Data model, §M1-d
Depends on: —
Acceptance:
  - numbered forward-only SQL migrations create `nodes`, `tenant_quota`, `instances`,
    `idempotency_keys`, `outbox` with the columns and constraints in ADR-0001 §Data model
  - `make db-up` / `make db-down` start/stop a Postgres 16 container; `make test-integration`
    runs `go test -race -count=1 -tags=integration ./...`
  - `internal/testutil` gives each test a fresh migrated database (`testutil.NewDB(t)`)
  - integration test `TestSchemaRejectsOverAllocation`: an UPDATE setting
    `allocated > capacity` fails with a check violation
  - integration test `TestSchemaRejectsOverQuota`: `used > quota_limit` fails
  - integration test `TestSchemaRejectsBadGPUCount`: `gpu_count = 3` fails
  - `make test` (no Docker) still passes
Out of scope: any query code beyond the migration runner

### S1-3 (1 pt) — Seed fake nodes
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: M1 has no node sync loop (that is M2), so placement needs inventory from somewhere
Design link: ADR-0001 §Context "How M1 works without a controller"; Discussion D15
Depends on: S1-2
Acceptance:
  - `make seed-nodes N=10` upserts N nodes (half H100, half A100, capacity 8,
    schedulable, failure_domain set) into `DATABASE_URL`
  - integration test `TestSeedNodesIsIdempotent`: running seed twice leaves N rows
  - integration test `TestSeedNodesNeverTouchesAllocated`: a node with `allocated = 4`
    still has 4 after re-seeding (D15 column ownership)
Out of scope: the M2 node sync loop; deleting nodes

### S1-4 (1 pt) — Node ranking: filter, best fit, tie-break
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: best fit keeps whole nodes free for 8-GPU requests (D7); ranking is pure logic, so it is tested without a DB
Design link: ADR-0001 §The create transaction step 3; Discussion D5, D7
Depends on: —
Acceptance:
  - `internal/scheduler` has pluggable filter and score functions and a pure
    `Rank(nodes, req) []NodeID`
  - `TestRankWorkedExample` reproduces the D5 table: 10 H100 nodes, 2-GPU request → n2 first,
    n5 and n7 excluded
  - table-driven `TestRankFilters`: wrong gpu_type, not schedulable, too few free → excluded
  - `TestRankSpreadsTenantAcrossFailureDomains`: on equal score, a node in a failure domain
    where the tenant has no instance ranks first
  - `TestFragmentedGPUs`: pure function returns free GPUs that cannot serve an 8-GPU request
  - `make test` passes
Out of scope: DB writes (S1-5)

### S1-5 (2 pts) — Place: atomic reserve with fallback
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: the snapshot may be stale; the conditional UPDATE is what actually prevents over-allocation
Design link: ADR-0001 §The create transaction step 3, §M1-a; Discussion D4, D5, D10
Depends on: S1-2, S1-4
Acceptance:
  - `func (s *Scheduler) Place(ctx context.Context, tx pgx.Tx, req Request) (NodeID, error)`
    reads a snapshot, ranks, and reserves with
    `UPDATE nodes … WHERE id=$1 AND schedulable AND allocated + $n <= capacity`
  - returns `ErrInsufficientCapacity` when no candidate reserves
  - integration test `TestPlaceFallsBackWhenBestNodeFills`: best node is filled between
    snapshot and reserve (via a hook) → next candidate chosen
  - integration test `TestPlaceInsufficientCapacity`: all nodes full → `ErrInsufficientCapacity`,
    no `allocated` changed
  - Place never locks more than one node row (no `FOR UPDATE` on the snapshot read)
  - `make test-integration` passes
Out of scope: quota, instance rows (S1-6)

### S1-6 (2 pts) — Create (happy path) and get instance
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: the core user flow; quota, placement, instance and outbox commit together or not at all
Design link: ADR-0001 §API (M1), §The create transaction
Depends on: S1-1, S1-5
Acceptance:
  - `cmd/apiserver` serves `POST /v1/instances` → `202` with state SCHEDULED, node_id,
    resource_version 1 and an `ETag`; `GET /v1/instances/{id}` → `200` same instance
  - one transaction in the documented lock order: quota row (created at 16 if missing)
    `used += n`, `Place`, INSERT instance, INSERT outbox `instance.scheduled`
  - `400 invalid_request` for missing `X-Tenant-ID`, missing `Idempotency-Key`,
    unknown gpu_type, gpu_count not in {1,2,4,8}; `TestCreateValidation` table-driven
  - integration test `TestCreateHappyPath`: after create, exactly 1 instance, 1 outbox row,
    `nodes.allocated` and `tenant_quota.used` increased by gpu_count
  - integration test `TestGetOtherTenantIs404`
  - integration test `TestCreateRollsBackOnFailure`: an injected error after `Place` leaves
    no instance, no outbox row, and unchanged `allocated`/`used`
Out of scope: 403/503 (S1-7), idempotency replay (S1-8), DELETE (S1-10)

### S1-7 (1 pt) — Create fails cleanly: quota 403, capacity 503
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: clients must tell "you are not allowed" (do not retry) apart from "try again later"
Design link: ADR-0001 §API (M1), §The create transaction steps 2–3; Discussion D2, D3
Depends on: S1-6
Acceptance:
  - integration test `TestCreateQuotaExceeded`: tenant at 16 used → `403 quota_exceeded`,
    nothing written (no instance, no outbox, no key, `allocated` unchanged)
  - integration test `TestCreateInsufficientCapacity`: all nodes of the type full →
    `503 insufficient_capacity`, `Retry-After: 30`, body names the gpu_type, nothing
    written, `tenant_quota.used` unchanged
  - quota is per GPU type: `TestQuotaIsPerGPUType` (16 H100 used, A100 create succeeds)
Out of scope: configurable quota limits; queueing (v2)

### S1-8 (2 pts) — Idempotent create
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: retried POSTs must never double-provision GPUs or double-charge quota (invariant 2)
Design link: ADR-0001 §The create transaction step 1, §M1-b
Depends on: S1-6
Acceptance:
  - `idempotency_keys` row inserted first in the create transaction, keyed by
    (tenant_id, key) with SHA-256 of the canonical parsed body
  - integration test `TestIdempotentReplay`: same key + same body → `202`, same id,
    `Idempotent-Replayed: true`, still 1 instance and 1 outbox row, quota counted once
  - `TestIdempotentReplayIgnoresFormatting`: same body with different whitespace /
    field order → replay, not 409
  - `TestIdempotencyKeyReused`: same key + different body → `409 idempotency_key_reused`
  - `TestIdempotencyKeyScopedToTenant`: same key from two tenants → two instances
  - `TestFailedCreateDoesNotKeepKey`: 503 then capacity frees → same key succeeds
Out of scope: key expiry; storing failed responses (ADR-0001 Q2)

### S1-9 (2 pts) — Lifecycle transitions with resource_version and release
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: the M2 controller will drive every state through this one function; it must be idempotent and release capacity exactly once
Design link: ADR-0001 §Lifecycle transitions (store.Transition); Discussion D10, D11, D12
Depends on: S1-1, S1-6
Acceptance:
  - `store.Transition(ctx, id, to, expectedRV, reason)` with errors `ErrNotFound`,
    `ErrInvalidTransition`, `ErrStaleVersion`
  - each success bumps resource_version by 1 and writes exactly 1 outbox row
    `instance.<state>` in the same transaction
  - integration test `TestTransitionFullLifecycle`: SCHEDULED → PROVISIONING → RUNNING →
    TERMINATING → TERMINATED gives 5 outbox rows total (incl. scheduled) and
    `allocated`/`used` back to their starting values
  - `TestTransitionToFailedReleasesCapacity` (from SCHEDULED and from PROVISIONING)
  - `TestTransitionStaleVersion`: wrong expectedRV → `ErrStaleVersion`, nothing changed
  - `TestTransitionRepeatIsNoop`: repeating an applied transition → error, no second outbox
    row, no second release
  - `TestTerminalNeverChanges`: any transition from TERMINATED/FAILED → `ErrInvalidTransition`
Out of scope: provisioning timeout and Pods (M2); HTTP exposure beyond DELETE (S1-10)

### S1-10 (1 pt) — DELETE moves instance to TERMINATING
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: users must be able to stop paying; a stale client must not override a newer state (invariant 4)
Design link: ADR-0001 §API (M1); open question Q1
Depends on: S1-9
Acceptance:
  - `DELETE /v1/instances/{id}` on PROVISIONING or RUNNING → `202` state TERMINATING
    via `store.Transition`; capacity is NOT released yet (invariant 5)
  - `If-Match: "<rv>"` stale → `409 stale_resource_version`; absent → current rv used
  - already TERMINATING/TERMINATED/FAILED → `202` current state, no new outbox row
  - SCHEDULED → `409 invalid_transition` until Q1 is decided
  - other tenant's or unknown id → `404`
  - integration tests `TestDelete*` cover each line above
Out of scope: driving TERMINATING → TERMINATED (M2 controller)

### S1-11 (2 pts) — Concurrency test: no over-allocation, no over-quota
Owner: test-engineer        Reviewer: verifier        Status: Proposed
Why: these races only show up under real parallel load; this test is the proof for invariants 1 and 6 with N replicas
Design link: ADR-0001 §Failure modes, §Invariants this design protects
Depends on: S1-2 (may fail until S1-7 lands)
Acceptance:
  - integration test `TestConcurrentCreatesNeverOverAllocate`: ≥100 goroutines, start
    barrier, several tenants, mixed gpu_count, total demand > fleet capacity, each goroutine
    using its own `pgxpool` connection (simulates replicas); asserts every node
    `allocated <= capacity` and invariant 7 (allocated = sum of active instances)
  - `TestConcurrentCreatesNeverExceedQuota`: ≥100 goroutines for ONE tenant asking for
    more than 16 GPUs total with ample capacity; asserts active GPUs ≤ 16 and
    `used` = sum of active instances; the rest got 403
  - `TestConcurrentLastGPUs`: 100 goroutines race for the final 2 GPUs → exactly one 202
  - every successful create has exactly one outbox row; tests pass with `-count=5`
Out of scope: chaos (M4); multiple OS processes

### S1-12 (2 pts) — Spec tests: idempotency races and state machine
Owner: test-engineer        Reviewer: verifier        Status: Proposed
Why: tests written from the spec, not the code, catch the implementation being wrong
Design link: ADR-0001 §The create transaction step 1, §Lifecycle transitions
Depends on: S1-2 (may fail until S1-8 and S1-9 land)
Acceptance:
  - integration test `TestConcurrentSameKeyCreatesOneInstance`: ≥100 goroutines, same key and
    body → one instance, one outbox row, all responses carry the same id
  - `TestConcurrentSameKeyDifferentBodies`: exactly one 202 body wins; others get 409 or
    replay of the winner
  - `TestConcurrentTransitionsOneWinner`: ≥100 goroutines call `Transition` with the same
    expectedRV → exactly one succeeds, the rest `ErrStaleVersion`; one outbox row
  - property test (`testing/quick` or rapid) over random transition sequences: state never
    moves backward, terminal never changes, `allocated`/`used` never negative
Out of scope: HTTP-level load tests

### S1-13 (1 pt) — M1 metrics and health endpoints
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: we need to see contention, fragmentation and accounting drift before chaos tests in M4
Design link: ADR-0001 §Observability
Depends on: S1-7, S1-9
Acceptance:
  - `/metrics` exposes every metric listed in ADR-0001 §Observability with the `gpucp_` prefix
  - `TestMetricsCreateResults`: one of each create outcome increments the matching
    `gpucp_create_results_total{result}` label
  - integration test `TestAllocationDriftGauge`: manually corrupting `allocated` makes
    `gpucp_store_allocation_drift_nodes` report 1 (alert, never auto-fix)
  - `/healthz` → 200; `/readyz` → 503 when the DB is unreachable (`TestReadyzDBDown`)
  - structured slog fields `instance_id`, `tenant`, `node_id`, `state` on create/transition logs
Out of scope: OpenTelemetry traces, dashboards, alert rules (M4)
