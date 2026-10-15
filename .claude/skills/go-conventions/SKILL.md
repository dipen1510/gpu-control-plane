---
name: go-conventions
description: Go coding conventions for gpu-control-plane — package layout, error handling, context and concurrency rules, database access, and testing style. Load before writing or reviewing any Go code in this repo.
---
# Go conventions

## Layout
- `cmd/<binary>/main.go` only wires dependencies and starts things; no logic.
- Logic lives in `internal/<pkg>`. One package per concept (`store`,
  `statemachine`, `scheduler`, `outbox`, `metering`), not per layer.
- Accept interfaces, return structs. Define interfaces where they are *used*.
- No package-level mutable state, no `init()` side effects.

## Errors
- Wrap with context: `fmt.Errorf("schedule instance %s: %w", id, err)`.
- Sentinel errors for conditions callers branch on:
  `var ErrConflict = errors.New("conflict")`; check with `errors.Is`.
- Never ignore an error. If it truly can't matter, `_ = x()` with a comment.
- Map domain errors to HTTP at the edge only (`ErrNotFound`→404,
  `ErrConflict`→409, `ErrStaleVersion`→409, validation→400, else 500).

## Context and concurrency
- Every function that does I/O or may block takes `ctx context.Context` first.
- Every goroutine has an owner that waits for it (`errgroup.Group` preferred)
  and a way to stop (ctx cancellation). No fire-and-forget goroutines.
- Never hold a mutex across I/O (DB, network, channel send that can block).
- Channels are bounded; document who closes each one (the sender).
- Prefer the database for cross-replica coordination (row locks,
  `SELECT … FOR UPDATE SKIP LOCKED`, compare-and-swap on `resource_version`).
  In-process mutexes do **not** protect against other replicas.
- Graceful shutdown: on SIGTERM, stop accepting work, drain in-flight work
  with a deadline, then exit.

## Database (Postgres, pgx v5)
- State change + outbox event in **one transaction**, always.
- Optimistic concurrency: `UPDATE … SET …, resource_version = resource_version + 1
  WHERE id = $1 AND resource_version = $2`; 0 rows → `ErrStaleVersion`.
- Migrations are numbered SQL files, forward-only, applied at startup in dev.
- Use `pgxpool`; set statement timeouts via ctx.

## Testing
- `make test` runs everything with `-race`. A test that only passes without
  `-race` is a bug.
- Table-driven tests with named cases: `t.Run(tc.name, …)`.
- Concurrency tests: ≥100 goroutines, a start barrier (closed channel), then
  assert the invariant (e.g. total allocated ≤ capacity).
- Integration tests needing Postgres use the `integration` build tag and a
  container; unit tests must run with no external dependencies.
- Assert on behavior, not on internals. Every test must be able to fail.
- Use `t.Cleanup`, `t.Context()`, and `testing/synctest` for time-dependent code.

## Logging and metrics
- `log/slog` with structured fields (`instance_id`, `tenant`, `state`).
- Prometheus metric names: `gpucp_<subsystem>_<name>_<unit>`.
