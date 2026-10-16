-- idempotency_keys: one row per successful create (ADR-0001 M1-b).
-- The key is inserted first in the create transaction (step 1), before the
-- instance row exists (step 4), so the FK is checked only at COMMIT.
CREATE TABLE idempotency_keys (
    tenant_id    text        NOT NULL,
    key          text        NOT NULL,
    request_hash bytea       NOT NULL,
    instance_id  text        NOT NULL
        REFERENCES instances (id) DEFERRABLE INITIALLY DEFERRED,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, key)
);
