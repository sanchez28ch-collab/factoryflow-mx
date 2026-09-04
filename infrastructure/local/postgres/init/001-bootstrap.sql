\set ON_ERROR_STOP on

BEGIN;

CREATE SCHEMA IF NOT EXISTS operational;
CREATE SCHEMA IF NOT EXISTS ingestion;
CREATE SCHEMA IF NOT EXISTS quality;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA operational IS
  'Transactional state for orders, lots, pieces and stations';

COMMENT ON SCHEMA ingestion IS
  'Landing and control structures for batch and streaming ingestion';

COMMENT ON SCHEMA quality IS
  'Data-quality rules, executions and detected violations';

COMMENT ON SCHEMA audit IS
  'Technical traceability, migrations and operational audits';

CREATE TABLE IF NOT EXISTS audit.platform_bootstrap (
    component TEXT PRIMARY KEY,
    installed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO audit.platform_bootstrap (component)
VALUES ('local-postgresql-foundation')
ON CONFLICT (component) DO NOTHING;

COMMIT;
