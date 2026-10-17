CLUSTER ?= gpu-cp
GO      ?= go

# Local Postgres for integration tests (ADR-0001 M1-d).
PG_CONTAINER ?= gpucp-postgres
PG_IMAGE     ?= postgres:16
# 127.0.0.1, not localhost: localhost may resolve to ::1 first and miss the
# container's IPv4 port mapping, and matches pg_isready below.
DATABASE_URL ?= postgres://gpucp:gpucp@127.0.0.1:5432/gpucp?sslmode=disable

# Fake nodes for `make seed-nodes` (ADR-0001: M1 has no node sync loop).
N ?= 10

.PHONY: test test-integration lint db-up db-down seed-nodes run kind-up kind-down deploy

test:
	$(GO) test -race -count=1 ./...

# Needs a running Postgres: `make db-up` first.
# -p 2 caps how many test binaries run at once. Each package's tests open their
# own pools against one Postgres (max_connections=100); with the default
# -p=GOMAXPROCS a many-core CI box could run enough packages in parallel to
# exhaust connections and fail with "too many clients" instead of a real bug.
test-integration:
	DATABASE_URL='$(DATABASE_URL)' $(GO) test -race -count=1 -p 2 -tags=integration ./...

# Every step must be able to fail the build: a lint step that can't fail
# checks nothing. golangci-lint is deliberately not run: we enable no linters
# beyond vet + staticcheck, and binaries built with an older Go refuse this
# Go 1.26 module.
STATICCHECK_INSTALL := go install honnef.co/go/tools/cmd/staticcheck@latest

lint:
	@unformatted="$$(gofmt -l .)"; if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:" >&2; echo "$$unformatted" >&2; exit 1; fi
	$(GO) vet ./...
	$(GO) vet -tags=integration ./...
	@command -v staticcheck >/dev/null 2>&1 || { \
		echo "staticcheck not installed; install with: $(STATICCHECK_INSTALL)" >&2; exit 1; }
	staticcheck ./...
	staticcheck -tags=integration ./...

# Start (or restart) the Postgres 16 container and wait until it accepts TCP
# connections. The image's init phase runs a socket-only server first, so
# pg_isready must use -h 127.0.0.1 to wait for the real one.
db-up:
	@if docker container inspect $(PG_CONTAINER) >/dev/null 2>&1; then \
		docker start $(PG_CONTAINER) >/dev/null; \
	else \
		docker run -d --name $(PG_CONTAINER) -p 5432:5432 \
			-e POSTGRES_USER=gpucp -e POSTGRES_PASSWORD=gpucp -e POSTGRES_DB=gpucp \
			$(PG_IMAGE) >/dev/null; \
	fi
	@for i in $$(seq 1 30); do \
		docker exec $(PG_CONTAINER) pg_isready -h 127.0.0.1 -U gpucp -d gpucp >/dev/null 2>&1 && \
			{ echo "postgres ready: export DATABASE_URL='$(DATABASE_URL)'"; exit 0; }; \
		sleep 1; \
	done; \
	echo "postgres in $(PG_CONTAINER) did not become ready" >&2; docker logs --tail 20 $(PG_CONTAINER) >&2; exit 1

# Upsert N fake GPU nodes (half H100, half A100) into DATABASE_URL, applying
# migrations first. Re-running is safe and never changes `allocated` (D15).
seed-nodes:
	DATABASE_URL='$(DATABASE_URL)' $(GO) run ./cmd/seed-nodes -n $(N)

# Run the API server on :8080 against DATABASE_URL (applies migrations first).
run:
	DATABASE_URL='$(DATABASE_URL)' $(GO) run ./cmd/apiserver

# Stop and remove the container and its data.
db-down:
	docker rm -f $(PG_CONTAINER)

# kind-up is idempotent: creates the cluster only if missing, then (re)applies
# labels and the fake GPU capacity. The JSON-patch "add" on an existing key
# replaces it, so re-running is a no-op. Every kubectl call pins the kind
# context so it can never touch another cluster.
KCTX        := kubectl --context kind-$(CLUSTER)
GPU_PER_NODE ?= 8
# worker:type:zone
GPU_WORKERS := worker:H100:zone-a worker2:H100:zone-b worker3:A100:zone-a worker4:A100:zone-b

kind-up:
	@kind get clusters 2>/dev/null | grep -qx '$(CLUSTER)' \
		|| kind create cluster --name $(CLUSTER) --config deploy/kind/cluster.yaml --wait 120s
	@set -e; for w in $(GPU_WORKERS); do \
		n=$(CLUSTER)-$${w%%:*}; rest=$${w#*:}; t=$${rest%%:*}; z=$${rest#*:}; \
		$(KCTX) label node $$n gpu.example.com/type=$$t topology.kubernetes.io/zone=$$z --overwrite; \
		$(KCTX) patch node $$n --subresource=status --type=json \
			-p '[{"op":"add","path":"/status/capacity/gpu.example.com~1gpu","value":"$(GPU_PER_NODE)"}]'; \
	done
	$(KCTX) get nodes -L gpu.example.com/type -L topology.kubernetes.io/zone

kind-down:
	kind delete cluster --name $(CLUSTER)

deploy:
	@echo "deploy target is added in Milestone 2 (see deploy-kind skill)"; exit 1
