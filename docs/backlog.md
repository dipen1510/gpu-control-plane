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
Owner: architect        Reviewer: tech lead        Status: Proposed
Why: the tech lead should understand the whole system before any code exists.
Acceptance:
  - architect explains components, request flow, state machine, data model,
    and where concurrency / consistency get hard, in plain language
  - tech lead's questions are answered
  - no files changed outside docs/
Out of scope: Milestone 1 stories (A-2)

### A-2 (2 pts) — Milestone 1 ADR and story breakdown
Owner: architect        Reviewer: tech lead        Status: Proposed
Depends on: A-1
Acceptance:
  - docs/adr/0001-milestone-1-api-and-state.md exists with status Proposed,
    following the design-doc skill
  - Milestone 1 stories (S1-*) appended here, each 1–2 pts, each with
    verifiable acceptance criteria
Out of scope: writing any Go code
