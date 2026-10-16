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

### A-3 (1 pt) — Design update: M1 instant lifecycle driver
Owner: architect        Reviewer: tech lead        Status: Done
Result: Discussion D17 (+ D12 note), ADR-0001 Amendment 1, story S1-14 (Proposed),
S1-6/S1-8/S1-10/S1-11 updated
Depends on: A-2
Why: in M1 nothing moved an instance to TERMINATED, so DELETE never released GPUs or quota over HTTP
Acceptance:
  - D17 records options A (shortcut edges) vs B (instant driver) and the decision (B)
  - ADR-0001 records it as an amendment, not a silent rewrite; API table, M1-c and Q1 point to it
  - no files changed outside docs/
Out of scope: CLAUDE.md state-machine note (outside architect's write scope; orchestrator/tech lead)

### Milestone 1 stories (from ADR-0001)

Summary (details below). Integration tests need `make db-up` first.

| ID | Pts | Story | Owner | Depends on | Status |
|---|---|---|---|---|---|
| S1-1 | 1 | State machine rules | implementer | — | Done |
| S1-2 | 2 | Schema, migrations and test database | implementer | — | Done |
| S1-3 | 1 | Seed fake nodes | implementer | S1-2 | Done |
| S1-4 | 1 | Node ranking: filter, best fit, tie-break | implementer | — | Done |
| S1-5 | 2 | Place: atomic reserve with fallback | implementer | S1-2, S1-4 | Done |
| S1-6 | 2 | Create (happy path) and get instance | implementer | S1-1, S1-5 | Done |
| S1-7 | 1 | Create fails cleanly: quota 403, capacity 503 | implementer | S1-6 | Proposed |
| S1-8 | 2 | Idempotent create | implementer | S1-6 | Proposed |
| S1-9 | 2 | Lifecycle transitions with resource_version and release | implementer | S1-1, S1-6 | Proposed |
| S1-10 | 1 | DELETE ends at TERMINATED and releases capacity | implementer | S1-9, S1-14 | Proposed |
| S1-11 | 2 | Concurrency test: no over-allocation, no over-quota, create/delete churn | test-engineer | S1-2 | Proposed |
| S1-12 | 2 | Spec tests: idempotency races and state machine | test-engineer | S1-2 | Proposed |
| S1-13 | 1 | M1 metrics and health endpoints | implementer | S1-7, S1-9 | Proposed |
| S1-15 | 1 | `make lint` fails on real lint errors | implementer | — | Proposed |
| S1-14 | 1 | M1 instant lifecycle driver | implementer | S1-9 | Proposed |

S1-1, S1-2 and S1-4 can start in parallel. S1-11 and S1-12 are written from
the ADR in parallel with S1-5…S1-10 and S1-14 and are expected to fail until
those land.

### S1-15 (1 pt) — `make lint` fails on real lint errors
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: today `command -v staticcheck && staticcheck ./... || echo …` succeeds even when staticcheck
reports problems, and `golangci-lint … || true` hides that it can't run on Go 1.26 — a lint step
that can't fail checks nothing
Depends on: —
Acceptance:
  - `make lint` exits non-zero when staticcheck reports a finding (shown by a deliberate violation
    in a scratch copy) and when staticcheck is missing it prints how to install it and fails
  - staticcheck also runs with `-tags=integration`
  - golangci-lint is either removed or only run when its Go version supports the module
Out of scope: adding new linters

### S1-1 (1 pt) — State machine rules
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16)
Follow-ups from review: S1-9 must release capacity only when `HoldsCapacity(from) && IsTerminal(to)`
(ADR step 2 says "if to is terminal"; wrong for v2 PENDING→FAILED). Design question for M2:
no RUNNING→FAILED edge, so a node dying under a RUNNING instance looks like a normal termination.
Why: one place decides which transitions are legal, so the API and the M2 controller can never disagree
Design link: ADR-0001 §Lifecycle transitions; CLAUDE.md §Instance state machine
Depends on: —
Acceptance:
  - `internal/statemachine` defines the 7 states and `CanTransition(from, to State) bool`,
    `IsTerminal(State) bool`, `HoldsCapacity(State) bool`; no I/O, no DB imports
  - `TestCanTransition` is table-driven over all 49 (from, to) pairs and matches the
    CLAUDE.md diagram exactly (includes SCHEDULED → TERMINATING and PROVISIONING → TERMINATING;
    no backward edges)
  - `TestTerminalStatesNeverChange`: no edge leaves TERMINATED or FAILED
  - `make test` and `make lint` pass
Out of scope: persistence (S1-9)

### S1-2 (2 pts) — Schema, migrations and test database
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16)
Follow-ups from review: `make db-up`/`db-down` untested here (no Docker) — tech lead to run on Mac;
mark the test template `ALLOW_CONNECTIONS false` so an open psql session can't break test setup;
runner has no per-migration checksum (edits to applied migrations go unnoticed — fine for M1);
M3 relay must scan `WHERE published_at IS NULL`, never a "last id seen" cursor (outbox id order ≠
commit order); `make lint` can't fail on staticcheck findings (see S1-15).
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
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16)
Follow-ups from review: re-seeding sets schedulable=true (seed owns that column per D15), so it
un-cordons nodes — S1-11/S1-12 must not re-seed mid-test; validate N before connecting and reject
extra CLI args (small polish).
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
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16)
Follow-ups from review: ADR step 3 should list the D5 health tie-break; S1-13 must split nodes by
gpu_type before calling FragmentedGPUs; add an over-allocated-node case to TestFragmentedGPUs;
S1-5 passes a nil IntN (global, concurrency-safe rand) in production.
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
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE on 2nd review, 2026-10-16)
Review history: 1st review CHANGES REQUESTED — under READ COMMITTED, an UPDATE that waited on a
row lock and then matched 0 rows KEEPS that lock, so Place could hold 2+ node locks and deadlock
(40P01 in 5/40 runs). Fixed with a savepoint per reserve attempt (rollback releases the lock);
TestPlaceReleasesLockOnLostRace reproduces the contended path (fails 10/10 without the fix).
Follow-ups: ADR-0001 step 3 text "losing candidates are not locked" needs an amendment (savepoint);
S1-11 must fail on any 40P01; assign `gpucp_scheduler_place_duration_seconds` to S1-13 (each
attempt now costs 3 round trips); export a holding-states list from statemachine.
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
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16; server timeouts + outbox total count fixed before commit)
Follow-ups from review: S1-7 must make TestCreateQuotaExceeded fail if the quota WHERE is dropped,
and should return fixed 400/413/408 messages (decoder errors currently leak socket addresses);
S1-8 should cap X-Tenant-ID / Idempotency-Key length (a ~6 KB tenant id gives 500 from the btree
limit); S1-12 should add a test that fails if the outbox insert moves outside the transaction;
S1-14 must update TestCreateHappyPath's total outbox count to the full event list (NOTE in test);
unmatched routes / wrong methods return plain-text 404/405, not the JSON envelope.
Why: the core user flow; quota, placement, instance and outbox commit together or not at all
Design link: ADR-0001 §API (M1), §The create transaction
Depends on: S1-1, S1-5
Acceptance:
  - `cmd/apiserver` serves `POST /v1/instances` → `202` with state SCHEDULED, node_id,
    resource_version 1 and an `ETag`; `GET /v1/instances/{id}` → `200` same instance
  - `GET` with `If-None-Match: "<rv>"` matching the current rv → `304 Not Modified`, no body
    (`TestGetConditional`; Discussion D16)
  - one transaction in the documented lock order: quota row (created at 16 if missing)
    `used += n`, `Place`, INSERT instance, INSERT outbox `instance.scheduled`
  - `400 invalid_request` for missing `X-Tenant-ID`, missing `Idempotency-Key`,
    unknown gpu_type, gpu_count not in {1,2,4,8}; `TestCreateValidation` table-driven
  - integration test `TestCreateHappyPath`: after create, exactly 1 instance, exactly 1
    `instance.scheduled` outbox row, `nodes.allocated` and `tenant_quota.used` increased by
    gpu_count
  - GET tests assert the instance as currently stored, not a hardcoded SCHEDULED / rv 1,
    so they keep passing once S1-14 moves it to RUNNING after create
  - integration test `TestGetOtherTenantIs404`
  - integration test `TestCreateRollsBackOnFailure`: an injected error after `Place` leaves
    no instance, no outbox row, and unchanged `allocated`/`used`
Out of scope: 403/503 (S1-7), idempotency replay (S1-8), DELETE (S1-10), GET showing
RUNNING after create (S1-14)

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
    `Idempotent-Replayed: true`, still 1 instance and 1 `instance.scheduled` outbox row,
    quota counted once; the replay does not re-run the lifecycle driver (no extra outbox rows)
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

### S1-10 (1 pt) — DELETE ends at TERMINATED and releases capacity
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: users must be able to stop paying and get their quota back; a stale client must not override a newer state (invariant 4)
Design link: ADR-0001 §API (M1), Q1 (resolved), Amendment 1; Discussion D17
Depends on: S1-9, S1-14
Acceptance:
  - `DELETE /v1/instances/{id}` on SCHEDULED, PROVISIONING or RUNNING → `202` with state
    TERMINATING; the handler calls the S1-14 driver, so a `GET` right after shows TERMINATED
    and `nodes.allocated` / `tenant_quota.used` are back to their pre-create values
    (`TestDeleteFromEachStateEndsTerminated`, one sub-test per starting state)
  - capacity is released only in the TERMINATING → TERMINATED transaction (invariant 5)
  - `If-Match: "<rv>"` stale → `409 stale_resource_version`, nothing changed; absent → current
    rv used, re-read and retried up to 3 times on `ErrStaleVersion`
  - already TERMINATED/FAILED → `202` current state, no new outbox row; TERMINATING (left by
    a crash) → `202`, driver finishes the walk to TERMINATED (`TestDeleteResumesTerminating`)
  - other tenant's or unknown id → `404`
  - integration tests `TestDelete*` cover each line above
  - after delete, the tenant can create again up to its full 16 GPUs (`TestDeleteFreesQuota`)
Out of scope: Pods and real shutdown delay (M2 controller)

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
  - `TestConcurrentCreateDeleteChurn` (may fail until S1-14 and S1-10 land): ≥50 goroutines
    loop create → delete through the lifecycle driver for several rounds, total demand over
    time > fleet capacity; asserts `allocated <= capacity` and `used <= quota_limit` at every
    sample, invariant 7 at the end, every instance ends RUNNING or TERMINATED (none stuck),
    and after all deletes `allocated` and `used` are back to 0
  - also in churn: deletes racing the create driver on the same instance → driver stops on
    `ErrStaleVersion`, instance still ends TERMINATED, released exactly once
  - every successful create has exactly one `instance.scheduled` outbox row, and each instance
    has exactly one outbox row per state it passed through; tests pass with `-count=5`
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

### S1-14 (1 pt) — M1 instant lifecycle driver
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: without it nothing reaches TERMINATED in M1, so DELETE never gives back GPUs or quota (invariant 5)
Design link: ADR-0001 Amendment 1; Discussion D17, D12
Depends on: S1-9
Acceptance:
  - `internal/lifecycle` (M1 only) with `AfterCreate(ctx, id, rv)` walking
    SCHEDULED → PROVISIONING → RUNNING and `Delete(ctx, id, expectedRV)` walking
    `<current>` → TERMINATING → TERMINATED, one `store.Transition` per step, each step using
    the rv returned by the previous one; the state machine (S1-1) is unchanged
  - the create handler calls `AfterCreate` after commit and before responding, on
    `context.WithoutCancel` + timeout; the response still reports state SCHEDULED, rv 1
  - integration test `TestCreateThenGetIsRunning`: POST → `202` SCHEDULED; immediate GET →
    RUNNING, rv 3
  - integration test `TestDeleteEndsTerminatedAndReleases`: from RUNNING, `Delete` → state
    TERMINATED, `allocated`/`used` back to pre-create values, released exactly once
  - `TestDriverOutboxOnePerStep`: create + delete gives exactly 5 outbox rows in order
    scheduled, provisioning, running, terminating, terminated
  - `TestDriverStopsOnError`: an injected `Transition` error (or `ErrStaleVersion`) at each
    step → logged with `instance_id`, `state`, error; instance left in its last committed
    state; no further steps; create still returns `202`
  - `TestDriverLosesToDelete`: a DELETE that wins the race makes `AfterCreate` stop on
    `ErrStaleVersion`; instance ends TERMINATED
  - no `time.Sleep`, timers or goroutines outliving the request in `internal/lifecycle`
    (verifier greps); the driver is injected into the handler so it can be swapped for the
    M2 controller
  - `make test`, `make lint`, `make test-integration` pass
Out of scope: crash recovery of stuck instances (M2 controller); delays, timeouts, boot
failures (M2); HTTP DELETE wiring (S1-10)
