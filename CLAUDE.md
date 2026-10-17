# gpu-control-plane — project memory

Every agent and every session reads this file first. Keep it short and true.

## What we are building

A small GPU cloud control plane. Users request GPU instances through an API; a
scheduler places them on a fleet of (fake) GPU nodes; a Kubernetes controller
drives each instance through its lifecycle; lifecycle events feed a usage
metering pipeline.

```
 client ──► API server ──► Postgres (source of truth + outbox)
                │                    │
                │                    └──► outbox relay ──► event bus (NATS JetStream)
                │                                              │
                ▼                                              ▼
           scheduler (leader-elected)                  metering consumer ──► usage table
                │
                ▼
     GPUInstance CRD ──► controller (leader-elected) ──► Pods on kind "GPU" nodes
```

## Instance state machine

```
PENDING → SCHEDULED → PROVISIONING → RUNNING → TERMINATING → TERMINATED
   │           │            │
   └───────────┴────────────┴──► FAILED  (terminal; capacity is released)

also: SCHEDULED → TERMINATING, PROVISIONING → TERMINATING   (user deletes before RUNNING)
      RUNNING → FAILED   (M2, S2-9: Pod or node lost; ADR-0002)
```
Rules: transitions only move forward; every transition is a single DB
transaction that also writes an outbox event; a terminal state never changes.
v1: create returns 202 at SCHEDULED (PENDING unused until the v2 queue); the
controller drives later states asynchronously; boot/shutdown delays live in
the Pod, never in `time.Sleep` in our code. See `docs/Discussion.md` D12.
M1 only: an instant lifecycle driver in the apiserver walks SCHEDULED→PROVISIONING→RUNNING
after create and →TERMINATING→TERMINATED on DELETE via store.Transition (D17); replaced by
the controller in M2.

## Core invariants (the verifier checks these on every story)

1. **No GPU over-allocation**: allocated GPUs on a node never exceed its capacity, even with N scheduler replicas.
2. **Idempotent create**: same `Idempotency-Key` + same body → same instance; different body → 409.
3. **No lost events**: every committed state change produces exactly one outbox row; consumers dedupe by event ID.
4. **Optimistic concurrency**: writes carry `resource_version`; stale writes are rejected, never silently applied.
5. **Safe deletion**: an instance is not removed until its capacity is released (finalizer).
6. **Quota under concurrency**: a tenant's GPUs in capacity-holding states never exceed its `quota_limit` per GPU type, even with N replicas (conditional `UPDATE tenant_quota` + CHECK).
7. **Accounting matches**: `nodes.allocated` = SUM(`gpu_count`) of capacity-holding instances on that node, and `tenant_quota.used` likewise per tenant and type; drift is reported (`gpucp_store_allocation_drift_nodes`), never auto-fixed.

## Tech stack

Go 1.26 · Postgres 16 · NATS JetStream · Kubernetes via kind · controller-runtime (kubebuilder)
· Helm · Prometheus + OpenTelemetry. Python only for one-off scripts.

## Repo layout

```
cmd/            entrypoints: apiserver, scheduler, controller, metering
internal/       private packages (store, statemachine, scheduler, outbox, ...)
api/            CRD types and OpenAPI spec
deploy/         kind config, Helm charts, manifests
test/chaos/     failure-injection tests
docs/adr/       architecture decision records
docs/backlog.md every story and its status
.claude/agents/ the engineering team
.claude/skills/ shared playbooks
```

## Commands

| Command | What it does |
|---|---|
| `make test` | `go test -race -count=1 ./...` (no DB needed) |
| `make test-integration` | needs `make db-up`; `-race -p 2 -tags=integration` against `DATABASE_URL` |
| `make lint` | `gofmt` check + `go vet` + `staticcheck` (plain and `-tags=integration`); fails on any finding |
| `make kind-up` / `make kind-down` | create / delete the local cluster |
| `make deploy` | build images, load into kind, helm upgrade (added in Milestone 2) |

## How the team works (non-negotiable)

The human is the **tech lead**. Nothing is coded, tested, or deployed without
their explicit approval of a specific story.

Every agent runs in one of two modes:

- **PROPOSE** (default). Explain what you plan to do and why, link it to the
  design, and split it into 1–2 point stories using the `story-planning` skill.
  Write nothing except proposals under `docs/`. End with the list of story IDs
  awaiting approval.
- **EXECUTE \<story-id\>**. Only when the prompt contains the tech lead's
  approval of that exact story ("complete S1-3"). Do that story and nothing
  else, then report: files changed, how it was verified, open questions.

The main session (orchestrator) relays each proposal to the tech lead, waits
for "complete <ids>", then invokes the owning agent in EXECUTE mode. After an
implementer finishes, the **verifier** always reviews before a story is marked
done in `docs/backlog.md`.

Teaching matters: proposals explain the *why* (tradeoffs, failure modes) in
plain language, because the tech lead is learning the design as we go.

## Conventions

- Follow the `go-conventions` skill for all Go code.
- One story = one commit: `S1-3: idempotent instance create`.
- Never mark a story done if tests fail or acceptance criteria are unmet.
