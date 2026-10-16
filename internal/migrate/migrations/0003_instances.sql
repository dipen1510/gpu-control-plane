-- instances: the customer-facing source of truth (Discussion D11).
-- state values must match the constants in internal/statemachine.
CREATE TABLE instances (
    id               text        PRIMARY KEY,
    tenant_id        text        NOT NULL,
    gpu_type         text        NOT NULL,
    gpu_count        integer     NOT NULL,
    -- NULL until placed (PENDING is reserved for the v2 queue).
    node_id          text        REFERENCES nodes (id),
    state            text        NOT NULL,
    resource_version bigint      NOT NULL DEFAULT 1,
    failure_reason   text        NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    -- Discussion D6: only whole power-of-two slices of an 8-GPU node.
    CONSTRAINT instances_gpu_count_valid
        CHECK (gpu_count IN (1, 2, 4, 8)),
    CONSTRAINT instances_state_valid
        CHECK (state IN ('PENDING', 'SCHEDULED', 'PROVISIONING', 'RUNNING',
                         'TERMINATING', 'TERMINATED', 'FAILED'))
);

CREATE INDEX instances_tenant_id_idx ON instances (tenant_id);
CREATE INDEX instances_node_id_idx ON instances (node_id);
