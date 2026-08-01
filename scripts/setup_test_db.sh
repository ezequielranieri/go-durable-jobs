#!/usr/bin/env bash
set -euo pipefail

# Aplica la migración al DB de test (jobs_test) de forma idempotente.
#
# NOTA IMPORTANTE: este script aplica el SQL crudo con psql -f solo para
# simplicidad del testing local. En el flujo real de despliegue se usa el CLI
# de golang-migrate, que trackea las versiones aplicadas en schema_migrations
# y por lo tanto NO requiere que el SQL sea idempotente. No asumas que todas
# las migraciones del proyecto deberían llevar IF NOT EXISTS: es un caso
# puntual para este setup de test.

docker exec -i go-durable-jobs-postgres-test psql -U jobs -d jobs_test \
  < migrations/0001_create_jobs_table.up.sql
