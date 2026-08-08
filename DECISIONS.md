# DECISIONS.md — Project constitution for `go-durable-jobs`

🇪🇸 [Versión en español](./DECISIONS.es.md)

This file is the source of truth for any agent (human or AI) working on this
repository. Every implementation must respect these rules. If a task conflicts
with something defined here, **stop and consult before proceeding**.

Each decision records: the rule (what the code MUST do), the alternatives
rejected and why, and the cost the project accepts for choosing it. This last
part matters: knowing what a decision costs is what lets you reopen it later
with a strong justification instead of a hunch.

Status legend: `Accepted` = settled; `Open` = proposal, do not implement.

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
- **HTTP:** standard `net/http` (`http.ServeMux` with Go 1.22+ method routing).
- **Migrations:** `golang-migrate`, versioned and reversible.
- **Observability:** `slog` for structured logs + Prometheus for metrics.
- **Tracing:** OpenTelemetry — Phase 2, don't implement yet.
- **Testing:** `testing` package + `testcontainers` + race detector (`go test -race`).
- **Local infra:** Docker Compose (Postgres + app only in the MVP).

**Why these choices (and what was rejected)**
- **Redis** is deliberately deferred to Phase 2 (see §4): the `idempotency_key` unique
  constraint already gives atomic, durable dedupe with nothing extra to operate. Redis would
  add an ultra-fast read-path check and rate limiting — valuable for a high-volume producer,
  not at MVP scale.
- **External queue libraries (Kafka, RabbitMQ, etc.)** are forbidden in the MVP: a Postgres
  table + `SKIP LOCKED` is enough for this workload, and it keeps the source of truth in one
  place. A broker is an operational cost the problem doesn't ask for yet.
- **`chi` rejected in favor of standard `net/http`**: the implementation uses
  `http.ServeMux` with Go 1.22+ method-based routing (`POST /jobs`, `GET /jobs/{id}`).
  Three routes don't justify a third-party router dependency: the stdlib now covers
  method matching and path wildcards, which were the historical reasons to reach for
  `chi`. No middleware chain exists today that would need its request-scoped context
  helpers.
- **`testcontainers` over a hand-rolled harness**: gives real Postgres semantics (locking,
  `SKIP LOCKED`) in CI with per-test cleanup, instead of a mock that would test the code but
  not the guarantee.
- **OpenTelemetry** is an explicit deferral to Phase 2 (§4): for a single process,
  distributed tracing is over-engineering; it earns its cost only when the system splits
  into multiple services.
- **`slog` + Prometheus over a logging framework**: stdlib first. `slog` covers structured
  logs; Prometheus covers metrics; a third-party logging framework adds a dependency without
  a gap to fill.

---

## 2. Confirmed design decisions (don't reopen without strong justification)

> Status of every subsection: Accepted — do not reopen without strong justification (see preamble).

### 2.1 Idempotency — Status: Accepted

**Rule**
- Unique constraint on `idempotency_key` at the database level.
- If `POST /jobs` receives a key that already exists: respond **`200 OK`** with the existing
  job in the body (NEVER a 500). This mimics Stripe: a retrying client expects success, not
  a different error to handle.

**Alternatives rejected**
- *Application-level check-then-insert* (SELECT, then INSERT): racy. Two concurrent requests both pass the check and both insert → duplicate jobs. Only the DB constraint is atomic.
- *Redis SETNX as the dedupe source*: fast, but adds infrastructure and stops being truthful after a crash — Postgres is the source of truth, Redis is not. Left for Phase 2 as a hot-path *optimization*, never as the guarantee.
- *Natural-key idempotency* (`type` + hash of `payload`): two legitimately identical jobs become indistinguishable. The opaque client key is the cleaner contract.
- *201 on first creation, 200 on replay*: forces the client to handle two success codes; Stripe's single success answer is simpler to consume.

**Cost accepted**
- Every `INSERT` contends on the unique index; at very high enqueue throughput that index becomes a hotspot (irrelevant at MVP scale, revisit in Phase 2).
- `200 OK` on replay does not tell the client whether the job is still pending or already done — `GET /jobs/{id}` is the status probe, not the POST response.
- The client owns the key's lifecycle: if it loses the key, it cannot dedupe.

### 2.2 Job dequeue — Status: Accepted

**Rule**
- Use **`SELECT ... FOR UPDATE SKIP LOCKED`** so workers pick up jobs without race
  conditions.
- The query MUST explicitly filter `available_at <= NOW()` in the `WHERE` (don't rely only
  on the partial index) so delay works correctly.
- The dequeue order is **`priority DESC, created_at ASC`**: `priority` is a first-class
  contract field on `POST /jobs` (default `0`, higher wins) and `delay_seconds` on the
  request is stored as `available_at = NOW() + delay`, so a delayed job simply isn't
  visible until its time. The partial index `idx_jobs_status_available` covers this
  ordering without an extra sort.

**Alternatives rejected**
- *Postgres `LISTEN`/`NOTIFY`*: event-driven instead of polling, but notifications have no delivery guarantee — a worker that subscribes late silently misses events, and you need a polling fallback anyway. 500ms polling is simpler and lossless.
- *Advisory locks (`pg_advisory_lock`)*: workable, but you hand-roll queue semantics (ordering, delays, dead-letter) that `SKIP LOCKED` gives you natively.
- *Claim via `UPDATE ... WHERE id IN (SELECT ...)` without row lock*: two workers can read the same row and both claim it — the exact race this design exists to prevent.
- *Single serial worker*: zero contention, but zero parallelism; defeats the purpose.

**Cost accepted**
- Polling adds latency: a runnable job may wait up to `POLL_INTERVAL` (500ms) before being picked up. Acceptable for async work, irrelevant for batch jobs.
- Under heavy contention, `SKIP LOCKED` silently skips locked rows — the queue stays correct, but tail latency can stretch; the lock window is one row and short, so in practice this only shows in saturated queues.
- Each dequeue is a *write* (row lock + state change), so the dequeue path does not scale as pure reads would — inherent to any claim-based queue.

### 2.3 Dead Letter Queue (DLQ) — Status: Accepted

**Rule**
- NOT a separate table or queue. It's the same `jobs` table with `status = 'dead'`.
- There must be a way to requeue: `POST /jobs/{id}/requeue` endpoint.
  - Only allows requeueing if `status = 'dead'` (explicit guard in the UPDATE's `WHERE`).
  - If the job is not in `dead`: respond **`409 Conflict`**.
  - On requeue: `status = 'pending'`, `attempts = 0`, `last_error = NULL`,
    `updated_at = NOW()`, `available_at = NOW()`.

**Alternatives rejected**
- *Separate dead-letter table/queue*: adds a second persistence path and a transfer step; the job's history (`attempts`, `last_error`) would be split across tables and harder to inspect. The same `jobs` table with `status = 'dead'` keeps one source of truth and one migration.
- *Physical `DELETE` on final failure*: destroys the record — the project's explicit value is preserving history (soft states `dead`, `completed`) and keeping the job available for inspection and manual requeue. A DELETE makes the DLQ invisible and unrecoverable.
- *External dead-letter queue (broker-side DLQ)*: forbidden infrastructure in the MVP — the same reason as the main queue.

**Cost accepted**
- `dead` jobs stay in the table indefinitely until requeued — there's no TTL or archival, so the table grows with history (bounded only by operational hygiene).
- Any query that wants only "live" jobs must filter by status; status is a soft signal, so a missed `WHERE status = ...` silently mixes `dead` jobs back into the queue.
- Requeue is a manual, explicit operation (a `POST` endpoint), never automatic — a `dead` job stays dead unless someone acts.

### 2.4 Circuit Breaker — Status: Accepted (deliberately NOT implemented in the MVP)

**Rule**
- NOT implemented in the MVP. Jobs are generic and don't call external services by default.
  If a job type that does call external APIs is added in the future, document there where
  and why a circuit breaker would be added.
- This decision must remain explicit in the README's retrospective, not silently omitted.

**Alternatives rejected**
- *Implement it now for a generic case*: MVP jobs don't call external services, so a breaker would protect nothing — pure added complexity with no failure it can trip on.
- *Implement it in the dequeue/enqueue machinery*: wrong layer — the README retrospective is explicit that if a future job type calls external APIs, the breaker lives in that job's business handler (per host/API, with a failure threshold and cooldown before opening), never in the queue machinery, which isn't the layer that fails in that scenario.

**Cost accepted**
- No protection exists for a future external-calling job type; adding it later is a deliberate, documented step, not something retrofittable without touching the job's handler.
- If an external-dependent job arrives and the §2.4 note is forgotten, nothing in the framework hints at the breaker — it stays a documented gap, not an enforced one.

### 2.5 Retries — Status: Accepted

**Rule**
- Exponential backoff + jitter.
- `max_attempts` configurable per job (default 5). When exceeded, the job goes to
  `status = 'dead'`.
- A **panic in the job handler is treated as a retryable failure**: `ProcessJob` recovers
  it, records the panic in `last_error`, and marks the job failed with the normal backoff
  (or `dead` when attempts are exhausted). A panicking handler must never kill the worker
  or crash the pool.

**Alternatives rejected**
- *Fixed retry count without backoff*: retrying immediately re-hammers the failing dependency or DB — the README's premise (transient blips become permanent loss, persistent errors saturate the system) argues for spacing attempts out.
- *Retry forever without a limit*: a persistent error retries endlessly and saturates the system; the explicit boundary (`max_attempts`, default 5) is what moves the job into `dead`, where it's visible.
- *Immediate retry without jitter*: without jitter, N workers retrying the same failing job synchronize into thundering-herd spikes; backoff + jitter spreads them out.

**Cost accepted**
- Retry latency compounds: a job with `max_attempts` 5 can take minutes in total (base 1s+2s+4s+8s) before landing in `dead` — detection of a persistent failure isn't instant.
- `max_attempts` is a per-job default, not a global enforcement; each caller must consciously set it (or rely on the default), so a caller can pick pathological settings.
- During a backoff window the job sits in `pending` but isn't runnable until `available_at`; someone filtering only by status sees a misleading picture — the truly runnable set is `pending AND available_at <= NOW()`.

### 2.6 Graceful shutdown — Status: Accepted

**Rule**
- `context` + `signal.Notify`. The server must wait for active workers to finish their
  current job before exiting. Never cut a job off mid-way.
- An in-flight job runs on **`context.Background()`**, deliberately NOT the worker's
  cancelled context: `Pool.Shutdown` cancels the polling context to stop new dequeues, but
  the job currently being processed keeps its own non-cancellable context and runs to
  completion. This is what makes "never cut a job off mid-way" hold at the goroutine level
  even after a signal arrives.

**Alternatives rejected**
- *`context.Cancel` kill without drain*: cancelling mid-job aborts processing and can leave the job in the wrong state (`running` forever, or half-done) — the README's shutdown log shows the desired sequence: finish the in-flight job, then exit.
- *Force exit with active jobs*: killing the process with jobs in flight risks the acknowledgment path; with Postgres as source of truth the job is never lost, but its state stays `running` and stalls until manual intervention — nothing reaps it.
- *Fixed short drain, then SIGKILL*: a hard timeout can still cut a long job off mid-way, which is exactly what the rule forbids ("never cut a job off mid-way").

**Cost accepted**
- Shutdown time is bounded by the slowest in-flight job: if one job takes minutes, the process stays up that long (the `GRACE_PERIOD` drain window exists to cap this).
- During drain no new jobs are picked up, so a shutdown can briefly stall the queue — throughput dips while the pool winds down.
- `signal.Notify` + wait is a coordination pattern: it only works if every worker actually checks the context, so the guarantee is as strong as the code's discipline.

### 2.7 Integration test DB isolation — Status: Accepted

**Rule**
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

**Alternatives rejected**
- *Shared manual-use database*: any test that forgets to clean up pollutes live data and metrics — the README retrospective documents exactly this bug (leftover test jobs showed up as processed in the live metrics). Physical separation is the only airtight fix.
- *In-process sqlite mock for integration*: sqlite doesn't implement the Postgres locking semantics (`SKIP LOCKED`, `FOR UPDATE`) integration tests need; a mock would test the code, not the guarantee.
- *Separate logical schema in the same DB*: two schemas in one Postgres still share the server and its fsync/vacuum/disk; an accidental DSN or a shared role can still reach the manual data. A second service on its own port/volume (`5433`) removes the failure mode entirely.

**Cost accepted**
- A second Postgres service to run and maintain locally (extra container, own volume, own port) — setup requires the `postgres_test` service + `scripts/setup_test_db.sh`.
- Tests need a separate connection chain (`TEST_DATABASE_URL` → `DATABASE_URL` → default); the `DATABASE_URL` fallback is deliberate backward compatibility and, if misconfigured, can point tests at the manual DB — the guarantee holds only if the chain is respected.
- Per-test truncate adds setup time to every run (each test reseeds its data).

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
