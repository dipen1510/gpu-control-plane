-- outbox: one row per committed state change, written in the same
-- transaction (invariant 3). id is insertion order (allocated at INSERT, so
-- not strictly commit order); consumers dedupe on event_id.
CREATE TABLE outbox (
    id           bigserial   PRIMARY KEY,
    event_id     uuid        NOT NULL UNIQUE,
    aggregate_id text        NOT NULL,
    event_type   text        NOT NULL,
    payload      jsonb       NOT NULL,
    occurred_at  timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

-- The M3 relay scans only unpublished rows, in id order.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
