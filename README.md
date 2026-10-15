# gpu-control-plane

A small GPU cloud control plane in Go: API → scheduler → Kubernetes controller →
event-driven usage metering. Built story by story with a team of Claude agents
(`.claude/agents/`) and shared playbooks (`.claude/skills/`).

Start here: [`CLAUDE.md`](CLAUDE.md) for the design and team rules,
[`docs/backlog.md`](docs/backlog.md) for what is done and what is next.

## Prerequisites
Go 1.26+, Docker, kind, kubectl. Helm arrives in Milestone 2.

## Quick start
```sh
make kind-up   # 3-node local cluster with two labeled "GPU" workers
make test
```
