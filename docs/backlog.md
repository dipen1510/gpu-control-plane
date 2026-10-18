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

### A-4 (1 pt) — Record corrections found in review
Owner: architect        Reviewer: tech lead        Status: Done
Result: ADR-0001 Amendment 2 (lost-race lock + savepoint; Transition check order and
lock-then-CAS-then-release; API error codes and header limits; M3 outbox scan rule; invariants 6
and 7 evidence) and docs/Discussion.md D18 "Lessons from review (concurrency)".
Invariants 6 and 7 promoted into CLAUDE.md by the tech lead (2026-10-17), evidence: S1-11, S1-12.

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
| S1-7 | 1 | Create fails cleanly: quota 403, capacity 503 | implementer | S1-6 | Done |
| S1-8 | 2 | Idempotent create | implementer | S1-6 | Done |
| S1-9 | 2 | Lifecycle transitions with resource_version and release | implementer | S1-1, S1-6 | Done |
| S1-10 | 1 | DELETE ends at TERMINATED and releases capacity | implementer | S1-9, S1-14 | Done |
| S1-11 | 2 | Concurrency test: no over-allocation, no over-quota, create/delete churn | test-engineer | S1-2 | Done |
| S1-12 | 2 | Spec tests: idempotency races and state machine | test-engineer | S1-2 | Done |
| S1-13 | 1 | M1 metrics and health endpoints | implementer | S1-7, S1-9 | Done |
| S1-15 | 1 | `make lint` fails on real lint errors | implementer | — | Done |
| S1-16 | 2 | M1 cleanup: review follow-ups | implementer | S1-13 | Done |
| S1-17 | 1 | DB-down 503 everywhere + holding-states behavior tests | implementer | S1-16 | Done |
| S1-14 | 1 | M1 instant lifecycle driver | implementer | S1-9 | Done |

S1-1, S1-2 and S1-4 can start in parallel. S1-11 and S1-12 are written from
the ADR in parallel with S1-5…S1-10 and S1-14 and are expected to fail until
those land.

### S1-17 (1 pt) — DB-down 503 everywhere + holding-states behavior tests
Owner: implementer        Reviewer: verifier        Status: Done (verifier review 2026-10-17; ADR wording fixed before commit)
Result: GET 503 on DB down; pool connect timeout default 2s (store.ParsePoolConfig) → black-holed DB answers
503 in 2.0s; integration tests pin TERMINATING in both holding-state uses (spread, drift). 6/6 mutations caught.
Review: CHANGES REQUESTED only for the ADR row — a burst while the DB is down still yields 500 for requests
queued behind the pool (probe: 40 GETs → 24×500, 16×503). Row now states the limits.
Follow-up for M2: fail fast with 503 while connects are failing (pool stats/hooks, no string matching) —
test asserts zero 500s under a burst; cmd/seed-nodes reuse the pool config helper; warn at startup if
connect_timeout ≥ 10s; parallelize the black-hole subtests (+6s to make test).
Why: S1-16 review — GET still answers 500 when the DB is down; a black-holed DB times out at 10s as a
500 (no connect timeout); and a wrong holding-states list in scheduler or metrics would pass all tests
Depends on: S1-16
Acceptance:
  - GET /v1/instances/{id} with the DB unreachable → 503 unavailable, Retry-After: 5 (test)
  - pgxpool connect timeout set (≈2–3s) so a black-holed DB yields 503 well before the 10s request
    deadline (test with an unroutable/black-holed address or a fake dialer)
  - integration test: a TERMINATING instance still counts for tenant failure-domain spread in Place,
    and still counts as held so `gpucp_store_allocation_drift_nodes` stays 0 (fails if TERMINATING is
    dropped from either list)
  - ADR-0001 §Failure modes row "Postgres down or slow" matches the behavior
  - `make test`, `make lint`, `make test-integration` pass
Out of scope: new endpoints

### S1-16 (2 pts) — M1 cleanup: review follow-ups
Owner: implementer (docs part: architect)        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-17)
Result: DB unreachable → 503 unavailable, Retry-After: 5 (type/SQLSTATE detection, no string matching);
statemachine.HoldingStates() is the single list; tests pin lost-CAS-not-counted, deadlock-after-
TERMINATING → 202 and refuse-to-release; ADR-0001 Amendment 3. Implemented across a session restart.
Why: close the gaps reviews found in M1 so M2 starts from a clean base; two of them are behaviors no test guards
Depends on: S1-13
Acceptance:
  - test: a Transition that loses its CAS adds 0 to `gpucp_instance_transitions_total`
    (fails if the counter moves before the CAS)
  - test: DELETE where TERMINATING committed and the release step hits SQLSTATE 40P01 → 202, not 503
    (fails if the deadlock case is checked before the committed-TERMINATING branches)
  - create (and DELETE) with the DB unreachable → 503 (ADR §Failure modes), not 500; test with a closed pool
  - `promLogger` logs a readable error string, not `{}`; test asserts the message text
  - `statemachine` exports the capacity-holding state list; scheduler and metrics use it (no copies)
  - Transition "refuse to release if the returned row differs" branch has an in-repo test (S1-9 follow-up)
  - testutil skip message says 127.0.0.1, not localhost (S1-15 follow-up)
  - ADR-0001 amendment (architect): 503 conflict_retry + Retry-After: 1, extra create results
    (invalid, conflict_retry, error), stuck-TERMINATING gauge, DB-down 503
  - `make test`, `make lint`, `make test-integration` pass
Out of scope: /metrics on a separate port or in-flight limit (M2); new features

### S1-15 (1 pt) — `make lint` fails on real lint errors
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16)
Result: lint = gofmt check + go vet + staticcheck, each plain and with -tags=integration; any finding
or missing staticcheck fails. golangci-lint removed (no extra linters enabled; can't run on Go 1.26).
test-integration runs with -p 2 (closes the S1-11 max_connections follow-up). DATABASE_URL default
is 127.0.0.1 (localhost may resolve to ::1 and miss the container's IPv4 port).
Deviation accepted by tech lead (2026-10-17): gofmt check added. Follow-ups: testutil skip message still says
localhost; gofmt step ignores gofmt's own exit status (vet catches parse errors).
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
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16)
Follow-ups from review: ADR-0001 §API error column should list 413 request_too_large and 408
request_timeout; fixed "malformed JSON body" hides which field was wrong (a later story could name
the field safely); add a string-lookalike error case to TestCreateStoreErrorMapping; grammar
"1 free A100 GPUs".
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
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16)
Notes: store.Create returns CreateResult{Instance, Replayed}; S1-14's driver must run only when
Replayed is false. Header limits: X-Tenant-ID ≤ 64, Idempotency-Key ≤ 255, visible ASCII (no spaces).
Follow-ups from review: S1-12 should use a larger pool (test pool is 4 conns on 2 CPUs) so races
are real; a key whose instance is missing returns 500 (impossible in M1; mind it in M2 cleanup).
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
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE on 2nd review, 2026-10-16)
Review history: implementer found the ADR order (release, then CAS) makes concurrent losers hit
the used >= 0 CHECK (23514) → changed to lock quota → lock node → CAS on instance → winner releases.
1st review CHANGES REQUESTED: version must be checked before CanTransition, so a caller that lost a
race gets ErrStaleVersion (not ErrInvalidTransition) — S1-14/S1-10 rely on it. Fixed; added
PENDING→FAILED (no release) and a create/release churn test that fails on any 40P01.
Follow-ups: ADR amendment (A-4); the "refuse to release if the returned row differs" branch has no
in-repo test; the outbox-outside-tx mutation is still for S1-12.
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
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16; concurrent-delete 409 fixed before commit)
Review: verifier stress (4800 concurrent DELETEs, 600 create-vs-delete races) found no 500s and no
double release, but a DELETE without If-Match racing the create walk AND another DELETE could get 409
although the instance ended TERMINATED (23–29% in that race). Fixed: re-read before giving up and return
202 if terminal; TestDeleteConcurrentWithCreateWalk (400 DELETEs) fails 7–15% without the fix.
Decisions: no "re-DELETE" hint in the body (keep M1/M2 contract; document + S1-13 stuck metric);
with If-Match, losing a race stays 409. Follow-ups: If-Match reads only the first header line;
404/409 messages echo the unvalidated path id; walk budget starts before the first Get.
Notes from S1-14 review (must handle): Delete always tries →TERMINATING first, so resuming an
instance left in TERMINATING needs the caller to pass the current state (e.g. Delete(ctx, id, from,
rv)) and skip done steps; TERMINATED/FAILED → 202 without calling the driver. A step-2 error means
TERMINATING was committed — must not become 409, and the no-If-Match retry must re-read and resume,
not restart. Add Delete to the api.Lifecycle interface (keep the handler swappable for M2).
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
Owner: test-engineer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16)
Deviation accepted: "own connection per goroutine" is implemented as 4 independent replicas (own
pool/store/scheduler/driver/server, 8 conns each) sharing one Postgres — real contention within the
100-connection limit. Deadlock (no-savepoint) shape caught only ~1/10 here; TestPlaceReleasesLockOnLostRace
is the reliable guard (10/10).
Follow-ups from review: map 40P01 to 503 (ADR says so; today 500) — S1-13 or a small story; add
`-p 2` (or smaller pools) to test-integration so many-core CI can't exceed max_connections; assert the
churn race actually happened (cut > 0); soften the one_free_on_ten_nodes comment.
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
Owner: test-engineer        Reviewer: verifier        Status: Done (verifier review → property-test flake fixed, 2026-10-16)
Review: all 9 independent mutations caught (outbox in a separate tx before/after commit for Create and
Transition, check order, hash ignored on replay, extra state-machine edge, double release, CAS dropped).
CHANGES REQUESTED for a ~10% flake (random creates could exceed a tenant's quota; seed 1059). Fixed by
making the model predict quota/capacity refusals and assert them (now also covers invariant 6);
-count=50 clean; racer deadline 15s with fail-fast.
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
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16)
Result: all ADR §Observability metrics on a private registry + gpucp_store_stuck_terminating_instances
(> 1 min); DB gauges computed at scrape time, read-only, 2s timeout; drift reported never fixed;
route label = pattern; /healthz, /readyz (1s DB ping); SQLSTATE 40P01 → 503 conflict_retry,
Retry-After: 1, logged at Error. Extra create results: invalid, conflict_retry, error.
Follow-ups from review: (a) no test pins "transition counted only after commit" or "delete deadlock
after TERMINATING committed → 202" (both survive mutation) — add tests; (b) ADR amendment for
conflict_retry, extra results, stuck gauge; (c) promLogger logs {} for errors; (d) create with DB down
returns 500, ADR says 503; (e) /metrics on the API port without in-flight limit (revisit in M2);
(f) export holding-states list from statemachine (still duplicated).
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
Owner: implementer        Reviewer: verifier        Status: Done (verifier APPROVE 2026-10-16)
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

## Milestone 2 — Controller on kind (M2a / M2b / M2c)

Design: `docs/adr/0002-milestone-2-controller.md` (Proposed); reasoning:
`docs/Discussion.md` D19–D36 (accepted by the tech lead 2026-10-17).

### A-5 (2 pts) — Milestone 2 ADR, discussion record and M2a stories
Owner: architect        Reviewer: tech lead        Status: Done
Result: Discussion D19–D39; ADR-0002 (Accepted 2026-10-17, Q4 accepted: no `source` column now) incl. "Under the hood" (controller-runtime internals,
replaces the dropped learning story); M2 diagrams in docs/architecture.md; S2-1 … S2-19 below.
Tech lead answers (2026-10-17): Q1 inst_x → inst-x (D37), Q3 delete terminal CRs (D38), Q2 node sync
moved into M2a instead of a seed Job (D39 → S2-17 … S2-19)
Acceptance:
  - every accepted decision M2-1 … M2-16 has a D-entry with the why
  - ADR-0002 follows the design-doc skill and has a failure modes table and invariants impact
  - no files changed outside docs/
Out of scope: CLAUDE.md updates (state diagram RUNNING → FAILED, M1 driver note): orchestrator/tech lead

### M2a stories (single controller replica)

| ID | Pts | Story | Owner | Depends on | Status |
|---|---|---|---|---|---|
| S2-1 | 1 | kind cluster: 4 fake GPU workers | deployer | — | Done |
| S2-2 | 1 | Container image and `cmd/migrate` | implementer | — | Done |
| S2-3 | 2 | Postgres in kind via Helm + migrate Job | deployer | S2-1, S2-2 | Done |
| S2-4 | 2 | kubebuilder scaffold, GPUInstance CRD, RBAC, `cmd/controller` | implementer | — | Done |
| S2-5 | 2 | DB poller + `updated_at` index | implementer | S2-4 | Proposed |
| S2-6 | 1 | Fake GPU Pod spec builder | implementer | S2-4 | Done |
| S2-7 | 2 | InstanceReconciler happy path: SCHEDULED → PROVISIONING → RUNNING | implementer | S2-5, S2-6 | Proposed |
| S2-8 | 2 | Delete path: TERMINATING → TERMINATED with finalizer | implementer | S2-7 | Proposed |
| S2-9 | 1 | RUNNING → FAILED edge and `pod_lost` | implementer | S2-7 | Proposed |
| S2-10 | 2 | Boot failure and provisioning timeout | implementer | S2-8 | Proposed |
| S2-11 | 2 | TTL: `ttl_seconds` → `expires_at` → TERMINATING | implementer | S2-8 | Proposed |
| S2-12 | 2 | Remove the M1 driver; API DELETE stops at TERMINATING | implementer | S2-8 | Proposed |
| S2-13 | 1 | Workqueue tuning, sizing flags and controller metrics | implementer | S2-7 | Done |
| S2-14 | 2 | Helm chart (apiserver + controller), `make deploy`, smoke test | deployer | S2-3, S2-12, S2-18 | Proposed |
| S2-15 | 2 | Reconciler spec tests (envtest) from ADR-0002 | test-engineer | S2-4 | Proposed |
| S2-16 | 2 | End-to-end lifecycle tests on kind | test-engineer | S2-14, S2-19 | Proposed |
| S2-17 | 2 | `nodes.status` replaces `schedulable`; Place only on READY | implementer | — | Done |
| S2-18 | 2 | NodeReconciler: upsert GPU Nodes with READY / NOT_READY / CORDONED | implementer | S2-4, S2-17 | Done |
| S2-19 | 1 | Node sync edge cases: deleted node, capacity below allocated, type change | implementer | S2-18 | Proposed |

S2-1, S2-2, S2-4 and S2-17 can start in parallel. S2-15 is written from the ADR in parallel with
S2-7 … S2-11 and is expected to fail until they land.

### S2-1 (1 pt) — kind cluster: 4 fake GPU workers
Owner: deployer        Reviewer: verifier        Status: Proposed
Why: the controller needs real nodes with GPU type and capacity to pin Pods on (D14)
Design link: ADR-0002 §Context; Discussion D14, D26
Depends on: —
Acceptance:
  - `deploy/kind/cluster.yaml`: 1 control-plane + 4 workers
  - `make kind-up` labels workers `gpu.example.com/type` = H100, H100, A100, A100 and
    `topology.kubernetes.io/zone` (failure domain), and patches node status with extended resource
    `gpu.example.com/gpu: 8`; re-running it is a no-op
  - `kubectl get nodes -L gpu.example.com/type` shows the 4 types; `kubectl get node <w> -o
    jsonpath='{.status.capacity.gpu\.example\.com/gpu}'` prints 8 on each worker
  - `make kind-down` deletes the cluster; deploy-kind skill updated (4 workers)
Out of scope: Postgres (S2-3), node sync (M2b)

### S2-2 (1 pt) — Container image and `cmd/migrate`
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: Jobs in kind need a binary that applies migrations; every component ships from one Dockerfile
Design link: Discussion D26
Depends on: —
Acceptance:
  - `cmd/migrate` applies `internal/migrate` migrations to `DATABASE_URL`, idempotent (2nd run no-op;
    integration test `TestMigrateCommandIdempotent`)
  - multi-stage `Dockerfile` with `--build-arg CMD=<apiserver|controller|migrate>`, distroless or
    scratch runtime, non-root
  - `make test`, `make lint`, `make test-integration` pass
Out of scope: Helm (S2-3); node rows (node sync, S2-18; `seed-nodes` stays local-only and its
connect-timeout follow-up stays in M2b)

### S2-3 (2 pts) — Postgres in kind via Helm + migrate Job
Owner: deployer        Reviewer: verifier        Status: Proposed
Why: everything in M2 runs in the cluster; migrations must run before apiserver/controller start (D26)
Design link: Discussion D26; deploy-kind skill §2
Depends on: S2-1, S2-2
Acceptance:
  - Postgres 16 in namespace `gpucp` via Helm, PVC-backed, password in a Secret, readiness probe
  - migrate Job (Helm hook or ordered install) runs to Complete
  - `kubectl -n gpucp exec … psql -c '\dt'` lists the 5 M1 tables and `schema_migrations` shows
    every migration applied
  - deleting the Postgres Pod keeps data (PVC); re-running the Job is a no-op
Out of scope: HA Postgres, backups; node rows (filled by node sync, S2-18)

### S2-4 (2 pts) — kubebuilder scaffold, GPUInstance CRD, RBAC, `cmd/controller`
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: the CR gives kubectl visibility, Pod ownership and the finalizer (D19); the Manager is the
frame every later story plugs into
Design link: ADR-0002 §GPUInstance CRD, §Under the hood; Discussion D19, D34
Depends on: —
Acceptance:
  - `api/v1alpha1` GPUInstance types (spec/status as in ADR-0002), printer columns STATE, NODE,
    GPUS, AGE; generated CRD under `deploy/crds/`
  - `k8sName(id)` maps `inst_x` → `inst-x` (D37); table test incl. a DNS-1123 validity check; the
    original ID is in label `gpu.example.com/instance-id`; nothing parses the name back
  - RBAC markers: gpuinstances (+status, +finalizers), pods (get/list/watch/create/delete),
    nodes (get/list/watch), leases (for M2b)
  - `cmd/controller` starts a Manager with metrics on `:8081`, `/healthz`, `/readyz`, leader election
    off by default (flag), a no-op InstanceReconciler registered
  - envtest test: CRD installs; creating a GPUInstance is visible via the cache after WaitForCacheSync
  - `make test`, `make lint` pass (envtest binaries fetched by a make target)
Out of scope: reconcile logic (S2-7)

### S2-5 (2 pts) — DB poller + `updated_at` index
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: new instances exist only in Postgres; the poller is how the controller hears about them (D20)
Design link: ADR-0002 §Decisions M2-a, §Data model; Discussion D20
Depends on: S2-4
Acceptance:
  - migration: index on `instances (updated_at)` and partial index for non-terminal rows
  - `internal/controller/poller`: every 1s `SELECT id, now() … WHERE updated_at > $last − 5s` with
    `$last` = previous DB `now()`; every 30s all non-terminal ids; sends ids only, into a channel
    wired with `source.Channel`; intervals are flags
  - integration test `TestPollerSeesChangeCommittedLate`: a row whose tx started before the previous
    poll and committed after it is still reported (fails without the 5s overlap)
  - integration test `TestPollerResyncListsNonTerminal`; test that the poller issues no writes
  - poller DB errors are logged, counted, and retried next tick (no crash)
  - `make test`, `make lint`, `make test-integration` pass
Out of scope: LISTEN/NOTIFY

### S2-6 (1 pt) — Fake GPU Pod spec builder
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: boot delay, boot failure and shutdown delay live in the Pod, never in our Go code (D12, D35)
Design link: Discussion D35, D14
Depends on: S2-4
Acceptance:
  - pure `BuildPod(inst, cfg) *corev1.Pod`: name `k8sName(id)`, `spec.nodeName`, request and limit
    `gpu.example.com/gpu: N`, labels `app=gpu-instance` + `gpu.example.com/instance-id`, ownerRef to
    the CR, `restartPolicy: Never`
  - init container (busybox) sleeps random `BOOT_SECONDS` in [min,max] and exits 1 with probability
    `BOOT_FAIL_RATE`; main `sleep infinity`; `preStop` sleeps `SHUTDOWN_SECONDS`;
    `terminationGracePeriodSeconds` > shutdown
  - table test over the fields above; `grep -rn 'time.Sleep' internal/controller` finds nothing
  - `make test`, `make lint` pass
Out of scope: a custom image

### S2-7 (2 pts) — InstanceReconciler happy path: SCHEDULED → PROVISIONING → RUNNING
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: the core of M2 — the controller, not the API, drives an instance to RUNNING (D21, D28)
Design link: ADR-0002 §Reconcile, by DB state; Discussion D19, D21, D31
Depends on: S2-5, S2-6
Acceptance:
  - SCHEDULED: ensure CR with finalizer, ensure Pod (`AlreadyExists` = success), then
    `store.Transition` → PROVISIONING; PROVISIONING + Pod Ready → RUNNING; CR status patched from
    the returned row
  - Pod and CR informers map events to the instance-id key; poller ids feed the same queue
  - envtest `TestReconcileHappyPath` (test sets Pod Ready, envtest has no kubelet): states and
    outbox rows scheduled, provisioning, running in order
  - `TestReconcileIsIdempotent`: reconciling a RUNNING instance twice makes no DB write and no
    Kubernetes write
  - `TestReconcileCrashAfterPodCreate`: Pod exists, DB still SCHEDULED → converges, one Pod
  - `ErrStaleVersion` → requeue (not error log spam)
  - `make test`, `make lint`, `make test-integration` pass
Out of scope: delete (S2-8), failures (S2-9, S2-10)

### S2-8 (2 pts) — Delete path: TERMINATING → TERMINATED with finalizer
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: capacity must be released only once the Pod is really gone (invariant 5, D21)
Design link: ADR-0002 §Reconcile, by DB state; Discussion D19, D21
Depends on: S2-7
Acceptance:
  - TERMINATING: delete Pod (`NotFound` = success); when a live read confirms it is gone →
    TERMINATED (release); then delete CR and remove finalizer
  - `TestDeleteReleasesOnlyAfterPodGone`: while the Pod still exists, `allocated`/`used` unchanged
  - `kubectl delete gpuinstance` (envtest delete of the CR) on a RUNNING instance → DB TERMINATING →
    TERMINATED; finalizer removed only after the row is terminal (`TestCRDeleteHonorsFinalizer`)
  - terminal row with a lingering CR → CR deleted (`TestTerminalRowCleansUpCR`)
  - `make test`, `make test-integration` pass
Out of scope: API DELETE change (S2-12)

### S2-9 (1 pt) — RUNNING → FAILED edge and `pod_lost`
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: a lost RUNNING instance must not look like a user delete (D29); capacity is released (D25)
Design link: Discussion D25, D29
Depends on: S2-7
Acceptance:
  - `statemachine`: RUNNING → FAILED legal; `TestCanTransition` 49-pair table updated
  - RUNNING with Pod missing *confirmed by live read* or Pod phase Failed → delete Pod → FAILED
    `pod_lost`, released once
  - `TestPodLostUsesLiveRead`: cache says missing, API says present → no transition
  - CLAUDE.md diagram change flagged to the orchestrator (not edited here)
Out of scope: recovery (M2c)

### S2-10 (2 pts) — Boot failure and provisioning timeout
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: exercise the failure paths that D12 promised; capacity must come back
Design link: ADR-0002 §Reconcile, by DB state; Discussion D25
Depends on: S2-8
Acceptance:
  - init container terminated non-zero → delete Pod → FAILED `boot_failed`
  - PROVISIONING longer than `--provisioning-timeout` (default 5m) measured as `db_now −
    updated_at` → delete Pod → FAILED `provisioning_timeout`; before that, `RequeueAfter(remaining)`
  - `TestProvisioningTimeoutUsesDBTime` (row `updated_at` set in the past; no fake controller clock)
  - SCHEDULED with a Pod create rejected as invalid → FAILED `pod_create_failed`
  - capacity released exactly once in each test; `make test-integration` passes
Out of scope: retries of a failed boot

### S2-11 (2 pts) — TTL: `ttl_seconds` → `expires_at` → TERMINATING
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: instances that clean themselves up; the first controller-owned → TERMINATING (D28, D30)
Design link: Discussion D30
Depends on: S2-8
Acceptance:
  - migration adds `ttl_seconds`, `expires_at`; create accepts optional `ttl_seconds` (60 … 604800,
    else 400) and sets `expires_at = now() + ttl` in the create transaction
  - `ttl_seconds` is part of the idempotency hash (`TestIdempotencyKeyReusedDifferentTTL` → 409)
  - reconciler: before expiry `RequeueAfter(expires_at − db_now)`; at/after → TERMINATING
    `ttl_expired` from SCHEDULED, PROVISIONING or RUNNING (`TestTTLExpiry` per state)
  - GET shows `expires_at`; OpenAPI spec updated
Out of scope: extending a TTL

### S2-12 (2 pts) — Remove the M1 driver; API DELETE stops at TERMINATING
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: one owner per transition (D28); the driver was M1-only (D17, D36)
Design link: Discussion D28, D36; ADR-0001 Amendment 1
Depends on: S2-8
Acceptance:
  - `internal/lifecycle` deleted; create returns 202 SCHEDULED and a GET right after shows SCHEDULED
  - DELETE → TERMINATING only; on TERMINATING/TERMINATED/FAILED → 202 current state, no new outbox row
  - tests that needed later states (S1-10, S1-11 churn, S1-14) rewritten to call `store.Transition`
    directly; churn test still proves invariants 1, 6, 7
  - `gpucp_store_stuck_terminating_instances` threshold 5 min (ADR-0001 Amendment 3 revisit)
  - CLAUDE.md M1-driver note change flagged to the orchestrator
  - `make test`, `make lint`, `make test-integration` pass
Out of scope: anything in the controller

### S2-13 (1 pt) — Workqueue tuning, sizing flags and controller metrics
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: bounded retries protect Postgres and the API server during outages (D34)
Design link: ADR-0002 §Under the hood, §Observability; Discussion D34
Depends on: S2-7
Acceptance:
  - rate limiter = max(per-item exponential 5ms…5min, token bucket 10 qps / burst 100);
    `--instance-workers=8`, `--node-workers=2`, DB pool 16, kube QPS 50 / burst 100 as flags
  - unit test: a key failing repeatedly backs off up to the cap; success calls `Forget`
  - `/metrics` on :8081 shows `workqueue_depth`, `controller_runtime_reconcile_total` and the
    `gpucp_controller_*` metrics in ADR-0002 §Observability (`TestControllerMetricsExposed`)
Out of scope: priority queue

### S2-14 (2 pts) — Helm chart (apiserver + controller), `make deploy`, smoke test
Owner: deployer        Reviewer: verifier        Status: Proposed
Why: one command builds and runs the whole M2a system in kind
Design link: deploy-kind skill; Discussion D26
Depends on: S2-3, S2-12
Acceptance:
  - `deploy/helm/gpucp`: apiserver ×3 + Service, controller ×1, ServiceAccount + RBAC from S2-4,
    CRD, Pod env `BOOT_SECONDS`, `BOOT_FAIL_RATE` as values
  - `make deploy` builds images tagged with the git SHA, `kind load`s them, `helm upgrade --install`
  - smoke script: the 4 workers appear in `nodes` as READY; POST → RUNNING within 60s,
    `kubectl get gpuinstances` shows it, DELETE → TERMINATED, Pod and CR gone; exits non-zero on
    any failure
Out of scope: leader election and 3 controller replicas (M2b)

### S2-15 (2 pts) — Reconciler spec tests (envtest) from ADR-0002
Owner: test-engineer        Reviewer: verifier        Status: Proposed
Why: tests written from the spec catch the reconciler being wrong, not just different
Design link: ADR-0002 §Reconcile, §Failure modes
Depends on: S2-4 (fails until S2-7 … S2-11 land)
Acceptance:
  - table test over every DB state × Pod state (absent, pending, ready, init failed, deleting) asserting
    the next action from ADR-0002's table
  - restart test: new Manager over existing Pods and rows → no duplicate Pods, no wrong FAILED
    (proves WaitForCacheSync + live read)
  - race test: API DELETE vs controller → RUNNING on the same rv → one winner, one outbox row per state
  - invariant 7 holds after every test (allocated = SUM of holding instances)
Out of scope: real kubelet behavior (S2-16)

### S2-16 (2 pts) — End-to-end lifecycle tests on kind
Owner: test-engineer        Reviewer: verifier        Status: Proposed
Why: only a real kubelet runs init containers, preStop hooks and the extended-resource check
Design link: ADR-0002 §Failure modes
Depends on: S2-14
Acceptance:
  - `make test-e2e` (build tag `e2e`): create → RUNNING; delete → TERMINATED with GPUs and quota released
  - `BOOT_FAIL_RATE=1` → FAILED `boot_failed`, released
  - kill the controller Pod mid-boot → instance still reaches RUNNING after restart
  - delete a RUNNING instance's Pod by hand → FAILED `pod_lost`
  - `kubectl cordon` a worker → its row becomes CORDONED and creates of that type avoid it;
    `uncordon` → READY
  - at the end `gpucp_store_allocation_drift_nodes` = 0 and no orphan Pods
Out of scope: chaos suite (M4); stopping a kind node container (M2b / M4)

### S2-17 (2 pts) — `nodes.status` replaces `schedulable`; Place only on READY
Owner: implementer        Reviewer: verifier        Status: Done
Result: verifier approved 2026-10-17 (mutation test proves the reserve guard). Note for S2-18: every node upsert must set `status` explicitly (column default is READY).
Why: one column decides placement; a node that is not Ready must never get new GPUs (D39, invariant 1)
Design link: ADR-0002 §Data model changes, §Node sync, M2-e; Discussion D39, D32
Depends on: —
Acceptance:
  - migration: `status` with CHECK over the five D32 values, `status_changed_at`; backfill
    `schedulable` true → READY, false → CORDONED; drop `schedulable`
  - scheduler snapshot filters `status = 'READY'`; reserve `UPDATE … WHERE status = 'READY' AND
    allocated + n <= capacity`
  - integration test `TestPlaceSkipsNonReady` (NOT_READY and CORDONED excluded) and
    `TestPlaceRefusesNodeTurnedNotReady`: node marked NOT_READY between snapshot and reserve (hook)
    → falls back to the next node (fails if `status` is dropped from the reserve `WHERE`)
  - `seed-nodes`, `gpucp_scheduler_fragmented_gpus` and other users switched to `status`;
    `grep -rn schedulable internal cmd` finds only the migrations
  - `make test`, `make lint`, `make test-integration` pass
Out of scope: who writes `status` (S2-18)

### S2-18 (2 pts) — NodeReconciler: upsert GPU Nodes with READY / NOT_READY / CORDONED
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: inventory is discovered from Kubernetes, not typed in (D15); replaces the seed Job idea (D39)
Design link: ADR-0002 §Node sync; Discussion D23, D39
Depends on: S2-4, S2-17
Acceptance:
  - NodeReconciler registered on the Manager, own queue, `--node-workers=2`, shared Node informer;
    Nodes without `gpu.example.com/type` ignored
  - upsert by name: `gpu_type` (insert only), `capacity` from `status.capacity`, `failure_domain` from
    the zone label, `last_seen`, `status` per ADR-0002 table; `status_changed_at` only on a change
  - the upsert SQL never mentions `allocated`: `TestNodeSyncNeverTouchesAllocated` (row with
    allocated 4 keeps 4 after sync)
  - envtest `TestNodeSyncStatus`: Ready → READY; cordon → CORDONED; Ready=False → NOT_READY; back → READY
  - re-syncing an unchanged Node writes nothing (`TestNodeSyncIdempotent`)
  - `make test`, `make lint`, `make test-integration` pass
Out of scope: deleted nodes and conflicts (S2-19); PROBATION / QUARANTINED (M2b)

### S2-19 (1 pt) — Node sync edge cases: deleted node, capacity below allocated, type change
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: these are the cases where a naive sync corrupts accounting or the fleet view (D39)
Design link: ADR-0002 §Node sync, §Failure modes; Discussion D39
Depends on: S2-18
Acceptance:
  - Node deleted → row kept, NOT_READY (`TestNodeDeletedMarksNotReady`); the row is never deleted
  - sweep after WaitForCacheSync and every 5 min marks rows with no GPU Node NOT_READY
    (`TestNodeSweepCatchesMissedDelete`); sweep does not run before cache sync
  - capacity reported below `allocated` → capacity unchanged, other columns synced,
    `gpucp_nodes_capacity_conflicts` = 1 (`TestNodeCapacityBelowAllocatedNotClobbered`)
  - type label changed → stored type kept, `gpucp_nodes_type_conflicts` = 1
  - `gpucp_nodes{gpu_type,status}` exposed
Out of scope: waking that node's instances on Node events (M2b)

### M2b epic (not split yet) — HA, nodes, drift
- Leader election via Lease, 3 controller replicas; failover test (D22)
- Node health policy on top of the M2a sync (S2-17 … S2-19): a node returning from NOT_READY goes
  PROBATION, then READY after a quiet period; flapping → QUARANTINED until an operator clears it (D32)
- Node informer also enqueues the node's instances via the CR `spec.nodeName` index (node NotReady
  wakes its instances)
- optional `source` column so the sweep can skip DB-only scale-test nodes (ADR-0002 Q4, if needed)
- k8s/DB drift detection: orphan Pods, missing Pods, CR ≠ DB; metrics + repair toward the DB (D24)
- S1-17 follow-ups: fail-fast 503 for bursts while the DB is down (test: zero 500s); seed-nodes
  connect timeout; warn if `connect_timeout ≥ 10s`; parallelize black-hole subtests (D36)

### M2c epic (not split yet) — Auto-recovery
- RECOVERING state; `HoldsQuota` / `HoldsNode` split of invariant 7 (D33)
- grace deadline on DB time; fence by force-deleting the old Pod; release old node, keep quota;
  re-place with the same scheduler; `max_recoveries`
- `recovery_policy` (`auto` default, `none` opt-out); `store.Recover` internal only
- no capacity: stay RECOVERING, retry every 30s up to 10 min, then FAILED `no_capacity` and release quota
