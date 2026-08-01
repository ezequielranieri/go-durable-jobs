-- IF NOT EXISTS: hace el SQL re-aplicable con psql -f para el script de setup
-- del DB de test (scripts/setup_test_db.sh), que aplica este archivo crudo sin
-- el trackeo de golang-migrate. En el flujo real de despliegue se usa
-- golang-migrate, que trackea versiones en schema_migrations y no requiere
-- que el SQL sea idempotente.
CREATE TABLE IF NOT EXISTS jobs (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    type            TEXT NOT NULL,
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending, running, completed, failed, dead
    priority        INT NOT NULL DEFAULT 0,
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 5,
    available_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key ON jobs(idempotency_key);

CREATE INDEX IF NOT EXISTS idx_jobs_status_available ON jobs(status, available_at, priority DESC)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_jobs_status_dead ON jobs(status)
    WHERE status = 'dead';
