# DECISIONS.md — Project constitution for `go-durable-jobs`

🇪🇸 [Versión en español](./DECISIONS.es.md)

This file is the source of truth for any agent (human or AI) working on this
repository. Every implementation must respect these rules. If a task conflicts with
something defined here, stop and consult before proceeding.

---

## 1. Tech stack (fixed, non-negotiable without explicit discussion)

- **Language:** Go 1.22+
- **Persistence:** PostgreSQL (source of truth, the only idempotency mechanism in the MVP)
- **Cache / Redis:** NOT used in the MVP. Reserved for Phase 2 (fast idempotency,
  rate limiting).
- **Concurrency:** Native worker pool with `channels`, `context`, and `errgroup`. External
  queue libraries (Kafka, RabbitMQ, etc.) are forbidden in the MVP.
- **Architecture:** Clean Architecture / Hexagonal. The domain does not depend on
  infrastructure.
- **HTTP:** `chi` or standard `net/http`.
- **Migrations:** `golang-migrate`, versioned and reversible.
- **Observability:** `slog` for structured logs + Prometheus for metrics.
- **Tracing:** OpenTelemetry — Phase 2, don't implement yet.
- **Testing:** `testing` package + `testcontainers` + race detector (`go test -race`).
- **Local infra:** Docker Compose (Postgres + app only in the MVP).

---

## 2. Confirmed design decisions (don't reopen without strong justification)

### 2.1 Idempotency
- Unique constraint on `idempotency_key` at the database level.
- If `POST /jobs` receives a key that already exists: respond **`200 OK`** with the existing
  job in the body (NEVER a 500). This mimics Stripe: a retrying client expects success, not
  a different error to handle.

### 2.2 Job dequeue
- Use **`SELECT ... FOR UPDATE SKIP LOCKED`** so workers pick up jobs without race
  conditions.
- The query MUST explicitly filter `available_at <= NOW()` in the `WHERE` (don't rely only
  on the partial index) so delay works correctly.

### 2.3 Dead Letter Queue (DLQ)
- NOT a separate table or queue. It's the same `jobs` table with `status = 'dead'`.
- There must be a way to requeue: `POST /jobs/{id}/requeue` endpoint.
  - Only allows requeueing if `status = 'dead'` (explicit guard in the UPDATE's `WHERE`).
  - If the job is not in `dead`: respond **`409 Conflict`**.
  - On requeue: `status = 'pending'`, `attempts = 0`, `last_error = NULL`,
    `updated_at = NOW()`, `available_at = NOW()`.

### 2.4 Circuit Breaker
- NOT implemented in the MVP. Jobs are generic and don't call external services by default.
  If a job type that does call external APIs is added in the future, document there where
  and why a circuit breaker would be added.
- This decision must remain explicit in the README's retrospective, not silently omitted.

### 2.5 Retries
- Exponential backoff + jitter.
- `max_attempts` configurable per job (default 5). When exceeded, the job goes to
  `status = 'dead'`.

### 2.6 Graceful shutdown
- `context` + `signal.Notify`. The server must wait for active workers to finish their
  current job before exiting. Never cut a job off mid-way.

### 2.7 Integration test DB isolation
- Integration tests run against a **physically separate** DB (`jobs_test`, `postgres_test`
  service in docker-compose, host port `5433`, own volume), never against the manual-use
  database (`jobs`, port `5432`).
- The per-test truncate (`truncateJobs`) is defensive hygiene, not the guarantee: even if a
  future test forgets to clean up, it's impossible for it to touch manual data.
- Test connection chain: `TEST_DATABASE_URL` → `DATABASE_URL` → default (`jobs_test`).
- Migration `0001` carries `IF NOT EXISTS` as a specific case so
  `scripts/setup_test_db.sh` (which applies raw SQL with `psql -f`) is re-runnable. This
  does NOT set a norm: the real flow uses golang-migrate, which tracks versions and doesn't
  require idempotent SQL.

---

## 3. Code conventions

- Domain errors and infrastructure errors are modeled separately (distinct types, never mix
  a raw Postgres error into the domain layer).
- All Postgres access goes through `internal/infrastructure/postgres`; SQL is never called
  directly from `handler` or `application`.
- Use cases live in `internal/application` and depend only on interfaces (ports) defined in
  `internal/domain`, never on concrete implementations.
- Table and column names in `snake_case`. Go type and function names in idiomatic style
  (`CamelCase` / `camelCase`).

## 4. Explicit prohibitions

- Don't add Redis, Kafka, RabbitMQ, Temporal, or any additional infra unless the reason is
  documented and explicitly approved.
- Don't implement Circuit Breaker or OpenTelemetry in the MVP.
- Don't return `500 Internal Server Error` for an expected idempotency conflict.
- No physical `DELETE` of jobs. History is preserved (soft states: `dead`, `completed`).

## 5. Reference roadmap (high level)

1. Structure + domain + Postgres + enqueue job + basic tests
2. Worker pool + `SKIP LOCKED` + states + graceful shutdown
3. Retries + DLQ + requeue
4. Prometheus metrics + structured logs + complete API
5. Race tests, testcontainers, benchmarks
6. Documentation and final retrospective in the README
