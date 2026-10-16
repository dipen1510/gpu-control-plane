# Architecture diagrams

Two views:
1. **Full system (M1–M4)** — everything we plan to build, each box tagged
   with the milestone that delivers it.
2. **Milestone 1 only** — what exists after M1, tagged with the stories that
   build each box.

Notation: solid arrows are calls/data flow, labelled with port and protocol.
Dashed arrows are future (v2) or test-only paths. Cylinders are data stores.
Tags: `[M1]`…`[M4]` milestone, `[v2]` deferred, `[S1-n]` story.

---

## 1. Full system design (M1–M4)

```mermaid
flowchart TB
    client(["Client<br/>curl / CLI / dashboard"])
    operator(["Operator<br/>kubectl · make targets"])

    subgraph mac["Developer Mac — Docker"]
        subgraph kind["kind cluster gpu-cp"]

            subgraph k8s_cp["Kubernetes control-plane node"]
                kapi["kube-apiserver :6443"]
                etcd[("etcd<br/>GPUInstance CRs · Pods · Leases")]
                kapi --- etcd
            end

            subgraph ns["Namespace gpucp — our control plane"]

                subgraph api_tier["API tier"]
                    svc["Service apiserver :80"]
                    api["apiserver ×3 [M1 code, M2 deployed]<br/>REST /v1/instances<br/>quota · idempotency · placement<br/>scheduler library: filter → score → reserve<br/>instant lifecycle driver (M1 only, removed in M2)"]
                end

                subgraph reconcile_tier["Reconcile tier [M2]"]
                    ctrl["controller ×3, 1 leader via Lease [M2]<br/>instance reconciler: SCHEDULED → … → TERMINATED<br/>node sync loop · provisioning timeout"]
                    sched["scheduler service + queue [v2]<br/>priority · fairness · aging"]
                end

                subgraph event_tier["Event tier [M3]"]
                    relay["outbox relay [M3]"]
                    nats[("NATS JetStream :4222 [M3]<br/>instance.* events")]
                    meter["metering consumer [M3]<br/>GPU-seconds per tenant<br/>dedupe by event_id"]
                end

                subgraph data_tier["Data tier"]
                    pg[("Postgres 16 :5432 [M1]<br/>instances · nodes · tenant_quota<br/>idempotency_keys · outbox<br/>usage [M3]")]
                end
            end

            subgraph gpu_tier["GPU worker nodes — fake GPUs [M2]"]
                w1["worker · H100<br/>gpu.example.com/gpu: 8<br/>Pods inst_* (boot / preStop delay)"]
                w2["worker2 · A100<br/>gpu.example.com/gpu: 8<br/>Pods inst_*"]
            end

            subgraph obs_tier["Observability and reliability [M4]"]
                prom["Prometheus · SLO alerts"]
                graf["Grafana dashboards"]
                otel["OpenTelemetry collector · traces"]
                chaos["chaos suite test/chaos<br/>kill leader · partition bus · create storms"]
            end
        end
    end

    client -->|"HTTP :8080 port-forward<br/>POST · GET If-None-Match · DELETE"| svc
    svc --> api
    api -->|"5432/tcp · one txn per request<br/>key → quota → node → instance → outbox"| pg

    ctrl -->|"5432/tcp · store.Transition()<br/>rv check + outbox row"| pg
    ctrl -->|"6443 · create/patch GPUInstance + Pod<br/>renew Lease · watch Nodes"| kapi
    kapi -->|"kubelet runs Pod<br/>spec.nodeName pinned"| w1
    kapi -->|"kubelet runs Pod"| w2

    relay -->|"read unpublished outbox rows"| pg
    relay -->|"4222 · publish"| nats
    meter -->|"4222 · durable subscribe"| nats
    meter -->|"5432/tcp · write usage"| pg

    sched -.->|"v2 · place PENDING instances"| pg

    prom -->|"scrape /metrics<br/>apiserver · controller · metering"| ns
    graf --> prom
    ns -.->|"OTLP traces"| otel
    chaos -.->|"kill pods · partition · load"| ns

    operator -->|"kubectl"| kapi
    operator -->|"make seed-nodes (M1 only)"| pg
```

### What each milestone adds

| Milestone | Adds to the picture |
|---|---|
| **M1** | apiserver code (HTTP API, scheduler library, state machine, store) + Postgres schema. Runs locally, not yet in kind. |
| **M2** | kind deployment, GPUInstance CRD, controller ×3 with leader election, node sync, Pods with boot/shutdown delays, provisioning timeout, injected boot failures, drift reconciler |
| **M3** | outbox relay, NATS JetStream, metering consumer, usage table; optional push notifications built from these events |
| **M4** | Prometheus, Grafana, OpenTelemetry, SLOs and alerts, chaos suite, postmortems |
| **v2** | queue (PENDING), separate leader-elected scheduler service, priority/fairness/aging, aligned GPU indices |

---

## 2. Milestone 1 only

In M1 everything runs **on the laptop without kind**: the apiserver as a
local process, Postgres in one Docker container. There is no controller, so
an **instant lifecycle driver** inside the apiserver walks the legal states
right away (D17): after create SCHEDULED → PROVISIONING → RUNNING, and on
DELETE → TERMINATING → TERMINATED, which releases GPUs and quota. Every step
goes through `store.Transition`, the same function the M2 controller will
call, so the state machine and outbox history are identical in M1 and M2.

```mermaid
flowchart TB
    client(["Client<br/>curl / CLI"])
    dev(["Developer / verifier"])

    subgraph mac["Developer Mac — M1 runs locally, no kind yet"]

        subgraph proc["apiserver process · go run ./cmd/apiserver · N replicas possible"]
            http["internal/api — HTTP handlers [S1-6 S1-7 S1-8 S1-10]<br/>POST /v1/instances → 202 SCHEDULED · 400 · 403 · 409 · 503<br/>GET /v1/instances/:id → 200 + ETag · 304 if unchanged<br/>DELETE /v1/instances/:id → 202, ends TERMINATED<br/>headers: X-Tenant-ID · Idempotency-Key · If-Match"]
            store["internal/store [S1-6 S1-9]<br/>Create() — one transaction<br/>Transition() — rv check · release capacity · outbox row"]
            sched["internal/scheduler — library [S1-4 S1-5]<br/>Rank: filter → best fit → tie-break<br/>Place: conditional UPDATE, fall back to next node"]
            sm["internal/statemachine [S1-1]<br/>CanTransition · IsTerminal · HoldsCapacity"]
            health["/metrics · /healthz · /readyz [S1-13]"]
            driver["instant lifecycle driver — M1 only [S1-14]<br/>after create: → PROVISIONING → RUNNING<br/>on DELETE: → TERMINATING → TERMINATED<br/>no sleeps · replaced by controller in M2"]

            http -->|"Create()"| store
            http -->|"after commit · on DELETE"| driver
            driver -->|"store.Transition() per step"| store
            store --> sched
            store --> sm
        end

        subgraph docker["Docker · make db-up"]
            pg[("Postgres 16 :5432 [S1-2]<br/>nodes · tenant_quota · instances<br/>idempotency_keys · outbox<br/>CHECK allocated ≤ capacity · used ≤ limit")]
        end

        seed["make seed-nodes N=10 [S1-3]<br/>upserts inventory, never writes allocated"]
        tests["make test · make test-integration<br/>unit · spec tests [S1-12]<br/>≥100-goroutine concurrency test [S1-11]"]
    end

    client -->|"HTTP :8080"| http
    store -->|"5432/tcp · lock order<br/>idempotency_keys → tenant_quota → nodes → instances → outbox"| pg
    sched -->|"snapshot SELECT, then<br/>UPDATE nodes WHERE allocated + n ≤ capacity"| pg
    seed -->|"5432/tcp"| pg
    dev --> seed
    dev --> tests
    tests -.->|"call store.Transition directly<br/>for FAILED and stale-version cases"| store
    tests -.->|"fresh database per test"| pg
```

### Create request in M1, step by step

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as apiserver (internal/api)
    participant S as store + scheduler library
    participant P as Postgres

    C->>A: POST /v1/instances<br/>X-Tenant-ID, Idempotency-Key, {gpu_type, gpu_count}
    A->>A: validate body (400 on error), hash parsed body
    A->>S: Create(req)
    S->>P: BEGIN
    S->>P: INSERT idempotency_keys ON CONFLICT DO NOTHING
    alt key already used
        P-->>S: 0 rows
        S->>P: ROLLBACK
        S-->>A: same hash → replay · different hash → 409
    else first use
        S->>P: UPDATE tenant_quota SET used = used + n WHERE used + n ≤ limit
        alt 0 rows
            S->>P: ROLLBACK
            S-->>A: 403 quota_exceeded
        else quota reserved
            S->>P: SELECT candidate nodes (snapshot, no locks)
            S->>S: rank: filter → best fit → tie-break
            loop each candidate until one succeeds
                S->>P: UPDATE nodes SET allocated = allocated + n WHERE allocated + n ≤ capacity
            end
            alt no candidate reserved
                S->>P: ROLLBACK
                S-->>A: 503 insufficient_capacity, Retry-After 30
            else node reserved
                S->>P: INSERT instances (SCHEDULED, rv 1)
                S->>P: INSERT outbox instance.scheduled
                S->>P: COMMIT
                S-->>A: instance (SCHEDULED, rv 1)
            end
        end
    end
    Note over A,P: success path only · M1 instant lifecycle driver (D17)<br/>after commit, before the response, one transaction per step
    A->>S: Transition(SCHEDULED → PROVISIONING, rv 1)
    S->>P: UPDATE instances … WHERE rv = 1 · INSERT outbox · COMMIT
    A->>S: Transition(PROVISIONING → RUNNING, rv 2)
    S->>P: UPDATE instances … WHERE rv = 2 · INSERT outbox · COMMIT
    A-->>C: 202 SCHEDULED + ETag "1" (or the error above · same contract as M2)
    C->>A: GET /v1/instances/:id
    A-->>C: 200 RUNNING, ETag "3"
```

### Delete request in M1

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as apiserver + driver
    participant S as store.Transition
    participant P as Postgres

    C->>A: DELETE /v1/instances/:id (optional If-Match)
    A->>S: Transition(RUNNING → TERMINATING, rv 3)
    S->>P: UPDATE instances … WHERE state = RUNNING AND rv = 3 · INSERT outbox · COMMIT
    alt stale If-Match or rv changed
        P-->>S: 0 rows
        S-->>A: ErrStaleVersion
        A-->>C: 409 stale_resource_version
    else transitioned
        A->>S: Transition(TERMINATING → TERMINATED, rv 4)
        S->>P: BEGIN · used -= n · allocated -= n · UPDATE instances WHERE rv = 4 · INSERT outbox · COMMIT
        Note over P: GPUs and quota released in the same transaction as TERMINATED
        A-->>C: 202 TERMINATING (same contract as M2)
    end
    C->>A: GET /v1/instances/:id
    A-->>C: 200 TERMINATED
```

### Instance states: M1 vs M2

Same state machine in both milestones; only *who* drives it and *how fast* changes.

```mermaid
stateDiagram-v2
    [*] --> SCHEDULED: POST (API, in-request placement)
    SCHEDULED --> PROVISIONING: M1 driver instantly · M2 controller creates Pod
    PROVISIONING --> RUNNING: M1 driver instantly · M2 Pod ready (10–30 s)
    RUNNING --> TERMINATING: DELETE
    SCHEDULED --> TERMINATING: DELETE
    PROVISIONING --> TERMINATING: DELETE
    TERMINATING --> TERMINATED: M1 driver instantly · M2 Pod gone · releases GPUs + quota
    SCHEDULED --> FAILED: M2 · Pod cannot be created
    PROVISIONING --> FAILED: M2 · timeout or boot failure
    TERMINATED --> [*]
    FAILED --> [*]
```

PENDING is not shown: it is used only by the v2 queue.

### In M1 vs not in M1

| In M1 | Not in M1 (milestone) |
|---|---|
| HTTP API: create, get (ETag / 304), delete | Controller, CRD, Pods, kind deployment (M2) |
| Scheduler library inside the apiserver | Leader election, node sync loop (M2) |
| State machine rules + `store.Transition` | Provisioning timeout, boot failures (M2) |
| Instant lifecycle driver (removed in M2) | |
| Postgres schema with CHECK constraints | Outbox relay, NATS, metering (M3) |
| Quota, idempotency, fail-fast 503 | Push notifications (M3+) |
| Outbox rows written (not yet published) | Prometheus/Grafana/OTel stack, chaos (M4) |
| Seed script for nodes | Queue and scheduler service (v2) |
| Unit, integration and concurrency tests | |
| `/metrics`, `/healthz`, `/readyz` endpoints | |
