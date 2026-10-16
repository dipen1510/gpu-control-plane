-- tenant_quota: per-tenant, per-GPU-type limit (Discussion D2). The row is
-- created on first use with the default limit.
CREATE TABLE tenant_quota (
    tenant_id   text    NOT NULL,
    gpu_type    text    NOT NULL,
    quota_limit integer NOT NULL DEFAULT 16,
    used        integer NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, gpu_type),
    -- Invariant 6 (candidate) backstop: quota can never be exceeded or go negative.
    CONSTRAINT tenant_quota_used_within_limit
        CHECK (used >= 0 AND used <= quota_limit)
);
