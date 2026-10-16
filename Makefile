CLUSTER ?= gpu-cp
GO      ?= go

# Local Postgres for integration tests (ADR-0001 M1-d).
PG_CONTAINER ?= gpucp-postgres
PG_IMAGE     ?= postgres:16
DATABASE_URL ?= postgres://gpucp:gpucp@localhost:5432/gpucp?sslmode=disable

.PHONY: test test-integration lint db-up db-down kind-up kind-down deploy

test:
	$(GO) test -race -count=1 ./...

# Needs a running Postgres: `make db-up` first.
test-integration:
	DATABASE_URL='$(DATABASE_URL)' $(GO) test -race -count=1 -tags=integration ./...

lint:
	$(GO) vet ./...
	$(GO) vet -tags=integration ./...
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed: go install honnef.co/go/tools/cmd/staticcheck@latest"
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || true

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

# Stop and remove the container and its data.
db-down:
	docker rm -f $(PG_CONTAINER)

kind-up:
	kind create cluster --name $(CLUSTER) --config deploy/kind/cluster.yaml
	kubectl label node $(CLUSTER)-worker  gpu.example.com/type=H100 --overwrite
	kubectl label node $(CLUSTER)-worker2 gpu.example.com/type=A100 --overwrite

kind-down:
	kind delete cluster --name $(CLUSTER)

deploy:
	@echo "deploy target is added in Milestone 2 (see deploy-kind skill)"; exit 1
