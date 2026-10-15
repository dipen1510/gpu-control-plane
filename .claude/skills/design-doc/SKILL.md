---
name: design-doc
description: How to write an Architecture Decision Record (ADR) for gpu-control-plane — numbering, required sections, and the bar for options, tradeoffs and failure modes. Use when the architect designs a milestone or changes a design decision.
---
# Writing an ADR

ADRs live in `docs/adr/NNNN-<slug>.md`, numbered from 0001, never renumbered.
Copy `template.md` from this skill's folder.

## The bar
- **Plain language first.** The tech lead is learning; explain each concept
  the first time it appears (e.g. "an outbox is a table we write events into
  in the same transaction as the state change, so we can't lose them").
- **Real options.** Each significant decision lists 2–3 options that a
  reasonable engineer might pick, with honest tradeoffs. Not strawmen.
- **Invariants named.** Say which invariant from `CLAUDE.md` each decision
  protects, and add new invariants if the design introduces them.
- **Failure modes.** For each component: what happens if it crashes, is slow,
  is duplicated, or sees stale data. Say how the design recovers.
- **Small.** 1–3 pages. If it is longer, the milestone is too big.

## Statuses
`Proposed` → `Accepted` (tech lead only) → later `Superseded by ADR-NNNN`.
An accepted ADR is not edited; a change gets a new ADR.
