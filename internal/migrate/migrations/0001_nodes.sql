-- nodes: GPU inventory. Inventory columns (gpu_type, capacity, schedulable,
-- failure_domain, last_seen) are written by the seed command in M1 and the
-- node sync loop in M2; allocated is written only by placement and release
-- (Discussion D15 column ownership).
CREATE TABLE nodes (
    id             text        PRIMARY KEY,
    gpu_type       text        NOT NULL,
    capacity       integer     NOT NULL,
    allocated      integer     NOT NULL DEFAULT 0,
    schedulable    boolean     NOT NULL DEFAULT true,
    failure_domain text        NOT NULL DEFAULT '',
    last_seen      timestamptz NOT NULL DEFAULT now(),
    -- Invariant 1 backstop: even a buggy query cannot commit an over-allocation.
    CONSTRAINT nodes_allocated_within_capacity
        CHECK (allocated >= 0 AND allocated <= capacity)
);
