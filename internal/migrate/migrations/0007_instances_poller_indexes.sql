-- Indexes for the controller's DB poller (ADR-0002 §Data model, Discussion D20).
-- The 1s poll selects ids WHERE updated_at > last - 5s; the 30s resync lists
-- every non-terminal id. Terminal rows pile up forever, so the partial index
-- keeps the resync proportional to live instances only.
-- Plain CREATE INDEX (not CONCURRENTLY): migrations run inside a transaction.
CREATE INDEX instances_updated_at_idx ON instances (updated_at);

CREATE INDEX instances_nonterminal_id_idx ON instances (id)
    WHERE state NOT IN ('TERMINATED', 'FAILED');
