-- nodes.status replaces the schedulable boolean (ADR-0002 M2-e, Discussion
-- D39, D32): one column decides placement, and only READY nodes get new GPUs.
-- M2a writes READY / NOT_READY / CORDONED; PROBATION and QUARANTINED are for
-- the M2b health policy and need no constraint change then.
ALTER TABLE nodes
    ADD COLUMN status text NOT NULL DEFAULT 'READY'
        CONSTRAINT nodes_status_valid
        CHECK (status IN ('READY', 'NOT_READY', 'PROBATION', 'CORDONED', 'QUARANTINED')),
    ADD COLUMN status_changed_at timestamptz NOT NULL DEFAULT now();

-- Backfill: a node that was not schedulable was taken out on purpose.
UPDATE nodes SET status = CASE WHEN schedulable THEN 'READY' ELSE 'CORDONED' END;

ALTER TABLE nodes DROP COLUMN schedulable;
