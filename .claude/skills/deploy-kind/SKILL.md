---
name: deploy-kind
description: Step-by-step procedure to build images and deploy gpu-control-plane to the local kind cluster, run a smoke test, and roll back. Use for any deploy or infra story.
---
# Deploying to kind

Cluster name: `gpu-cp` (see `Makefile`). Config: `deploy/kind/cluster.yaml`.
Four workers act as GPU nodes (`make kind-up`): `gpu-cp-worker`, `-worker2` = H100;
`-worker3`, `-worker4` = A100; zones alternate `zone-a`/`zone-b` (`topology.kubernetes.io/zone`);
each has extended resource `gpu.example.com/gpu: 8` patched into node status. `make kind-up` is
idempotent (re-run to restore labels/capacity).

## 0. Preconditions
```sh
docker info >/dev/null && kind version && kubectl version --client
kind get clusters | grep -q '^gpu-cp$' || make kind-up
kubectl config use-context kind-gpu-cp
```
Never run deploy commands against any context other than `kind-gpu-cp`.

## 1. Build and load images
Tag images with the short git SHA so every deploy is traceable:
```sh
TAG=$(git rev-parse --short HEAD)
docker build -t gpucp/<component>:$TAG -f Dockerfile --build-arg CMD=<component> .
kind load docker-image gpucp/<component>:$TAG --name gpu-cp
```
kind cannot pull from your local Docker daemon; `kind load` is required.

## 2. Apply in dependency order
1. CRDs (`kubectl apply -f deploy/crds/`) — wait for `Established`.
2. Postgres and NATS (dev charts under `deploy/`), wait for Ready.
3. Migrations job, wait for Complete.
4. API server, scheduler, controller, metering:
   `helm upgrade --install gpucp deploy/helm/gpucp --set image.tag=$TAG --wait --timeout 3m`

## 3. Smoke test
```sh
kubectl -n gpucp rollout status deploy --timeout=120s
kubectl -n gpucp port-forward svc/apiserver 8080:80 &
curl -fsS -X POST localhost:8080/v1/instances \
  -H 'Idempotency-Key: smoke-1' -d '{"gpu_type":"H100","count":1}'
# poll until state == RUNNING (≤ 60s), then DELETE and confirm TERMINATED
```

## 4. Roll back on failure
```sh
helm rollback gpucp            # previous revision
kubectl -n gpucp get events --sort-by=.lastTimestamp | tail -30
```
Report the failing step and the events; do not debug application code.

## Notes
- Helm charts, Dockerfile and CRDs are created by the stories that need them
  (Milestone 2). Until then, only step 0 applies.
