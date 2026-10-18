-- k8s_name: the Kubernetes object name of an instance's CR and Pod (D37, D40).
-- The controller's queue key is {namespace, k8s_name}; Reconcile finds the
-- row with WHERE k8s_name = $1. Postgres computes the value, so no code can
-- write a wrong one, and UNIQUE makes two ids that map to the same name
-- (e.g. inst_ab and inst-ab) impossible at insert time, so the lookup is
-- never ambiguous. The UNIQUE constraint's index serves the lookup.
ALTER TABLE instances
    ADD COLUMN k8s_name text GENERATED ALWAYS AS (replace(id, '_', '-')) STORED;

ALTER TABLE instances
    ADD CONSTRAINT instances_k8s_name_key UNIQUE (k8s_name);
