---
name: story-planning
description: How to split gpu-control-plane work into 1–2 point stories, the story template, IDs, and backlog statuses. Use whenever an agent proposes work or updates docs/backlog.md.
---
# Story planning

## Sizing
| Points | Meaning | Rough shape |
|---|---|---|
| **1** | One focused change, reviewable in ~10 min | one file or concept + its tests |
| **2** | A few files that only make sense together | e.g. migration + repository + tests |
| **>2** | Not allowed | split it |

Split by **behavior**, not by layer. "Create instance is idempotent" is a good
story; "write the repository layer" is not, because it can't be verified alone.

Splitting moves, in order of preference:
1. Happy path first, then each failure/edge case as its own story.
2. Read path before write path.
3. Single replica first, then the concurrency/HA story.
4. Hard-coded config first, then making it configurable.

## IDs
`S<milestone>-<n>` for build stories (S1-3), `A-<n>` for architecture /
learning stories, `C<milestone>-<n>` for chaos stories, `P-<n>` for
postmortem follow-ups. Never reuse an ID.

## Template (copy into docs/backlog.md)
```
### S1-3 (2 pts) — Idempotent instance create
Owner: implementer        Reviewer: verifier        Status: Proposed
Why: retried POSTs must never double-provision GPUs
Design link: ADR-0002 §Idempotency
Depends on: S1-2
Acceptance:
  - same Idempotency-Key + same body → same instance, 200
  - same key + different body → 409
  - make test (with -race) passes
Out of scope: expiry of old keys (S1-7)
```
Every acceptance criterion must be checkable by the verifier with a command
or a test name. "Works correctly" is not a criterion.

## Statuses
`Proposed` → `Approved` (tech lead said "complete <id>") → `In progress`
→ `In review` (verifier running) → `Done` (verifier APPROVE + tech lead ok)
or `Changes requested` (back to In progress).

Only the tech lead moves a story to `Approved`. Only a verifier APPROVE moves
it toward `Done`.

## Proposal footer
Every proposal ends with:
```
Awaiting approval: S1-3, S1-4
Reply "complete S1-3" (or several IDs) to start.
```
