CLUSTER ?= gpu-cp
GO      ?= go

.PHONY: test lint kind-up kind-down deploy

test:
	$(GO) test -race -count=1 ./...

lint:
	$(GO) vet ./...
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed: go install honnef.co/go/tools/cmd/staticcheck@latest"
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || true

kind-up:
	kind create cluster --name $(CLUSTER) --config deploy/kind/cluster.yaml
	kubectl label node $(CLUSTER)-worker  gpu.example.com/type=H100 --overwrite
	kubectl label node $(CLUSTER)-worker2 gpu.example.com/type=A100 --overwrite

kind-down:
	kind delete cluster --name $(CLUSTER)

deploy:
	@echo "deploy target is added in Milestone 2 (see deploy-kind skill)"; exit 1
