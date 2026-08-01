# go-durable-jobs

🇪🇸 [Versión en español](./README.es.md)

Durable job processor written in Go. Processes asynchronous jobs with at-least-once
delivery guarantees, idempotency, controlled retries, and basic observability.

> Status: MVP complete — architecture, tests, benchmarks, and retrospective documented.
> Phase 2 (Redis, inspection CLI, distributed tracing) not started — see the retrospective
> section below.

## Problem it solves

Any system that fires off asynchronous work (sending an email, charging a card, generating
a report, notifying an external webhook) eventually runs into three uncomfortable
questions:

- **What happens if the process crashes mid-job?** Without a durable queue, that job
  simply vanishes — nobody notices, nobody retries it.
- **What happens if the same job fires twice?** A network timeout makes the client retry
  a `POST`, and suddenly you charged the same card twice or sent the same email twice.
- **What happens when a job fails?** Without backoff retries, a transient error (the
  database blipped for a second, an external service had a hiccup) becomes a permanent
  loss. Without a retry limit, a persistent error saturates the system retrying forever.

`go-durable-jobs` answers those three questions using the simplest tools that provide the
right guarantee — without adding infrastructure (message queues, brokers) the problem
doesn't ask for yet:

- **Never lose a job:** Postgres as the source of truth. An enqueued job survives a
  process crash, a restart, or a deploy — it stays in the table, `pending`, waiting for a
  worker (the same one or another) to pick it up.
- **Never duplicate a job:** real idempotency via a unique constraint. The same
  `idempotency_key` never creates two jobs, no matter how many times the request arrives.
- **Never lose a job to a transient failure, without retrying forever:** exponential
  backoff with jitter and a configurable attempt limit. After exhausting retries, the job
  falls into an explicit dead letter queue (it doesn't disappear — it stays available for
  inspection and manual requeueing).

The project also serves as a deliberate exercise in **correct concurrency in Go**: the
core mechanism (`SELECT ... FOR UPDATE SKIP LOCKED`) is what guarantees that N workers can
compete for the same queue without stepping on each other — verified not just by code
review, but with tests run under `-race` and real scaling benchmarks (see the
[Benchmarks](#benchmarks) section).

## Design decisions

See [`DECISIONS.md`](./DECISIONS.md) for the full detail of confirmed technical decisions
(idempotency, dequeue, DLQ, circuit breaker, etc.) and their justification.

Quick summary:
- **Idempotency:** unique constraint in Postgres. Conflict → `200 OK` with the existing job.
- **Dequeue:** `SELECT ... FOR UPDATE SKIP LOCKED` + explicit `available_at` filter.
- **DLQ:** same table, `status = 'dead'`, with a requeue endpoint (`409` if not applicable).
- **Circuit Breaker:** not implemented in the MVP (see retrospective).

## How I used AI on this project

I used an AI assistant as a pair-programming tool during development —
not as an autopilot. The workflow I followed in practice:

- **Spec first, code after.** Before writing a single line, I locked
  down the stack, the data model, and the critical technical decisions
  (idempotency, dequeue, DLQ handling) in a design session. That became
  the basis for this README's "Design decisions" section.
- **Small tasks, mandatory review before moving forward.** Each piece
  (domain, repository, worker pool, HTTP, metrics) was planned, its
  plan reviewed before implementation, implemented, and verified with
  real evidence (tests running, clean `-race`, `curl` against the live
  server) before moving to the next. I never approved a piece just
  because "it compiled."
- **AI didn't replace the review judgment.** It was useful for
  generating plans, first-pass implementations, and tests — but
  finding the real problems required reading the code carefully,
  asking for raw evidence instead of summaries, and pushing back on
  "looks good" when something didn't add up.

Four concrete bugs this process caught before they reached `main`
(full detail in the [retrospective](#what-id-improve-today-honest-retrospective)):

1. a real race condition between picking up a job and marking it as
   running;
2. a bug that silently broke retries (the job never went back to
   `pending`);
3. an asymmetry in how success and failure were reported, which would
   have undercounted failure metrics;
4. data contamination between the test database and the manual-use
   database, caught because a number didn't add up, not because it was
   actively searched for.

None of these bugs were obvious on a first read of the generated code —
all of them came from requesting the plan before the code, and
verifying with real evidence (not trusting the summary) before
approving each piece.

## How to run the project

```bash
# 1. Postgres (manual-use database, host port :5432)
docker-compose up -d postgres

# 2. Apply the migration
docker exec -i go-durable-jobs-postgres psql -U jobs -d jobs \
  < migrations/0001_create_jobs_table.up.sql

# 3. Start the server
DATABASE_URL="postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable" \
  go run ./cmd/server
```

The server listens on `:8080` (or whatever `APP_PORT` is set to).

Environment variables (`internal/config/config.go`):

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | required | Postgres DSN |
| `APP_PORT` | `8080` | HTTP port |
| `NUM_WORKERS` | `5` | Number of workers in the pool |
| `POLL_INTERVAL` | `500ms` | Polling frequency for pending jobs |
| `GRACE_PERIOD` | `30s` | Drain window during graceful shutdown |
| `BASE_BACKOFF_DELAY` | `1s` | Base of the exponential retry backoff |

## How to test it

```bash
# 1. Enqueue a job → 201
curl -s -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"test.echo","payload":{"msg":"hello"},"idempotency_key":"demo-1"}'

# 2. Same idempotency_key → 200 with the SAME job (idempotency, not a duplicate)
curl -s -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"test.echo","payload":{"msg":"hello"},"idempotency_key":"demo-1"}'

# 3. Check the job's status → GET /jobs/{id} (id from the previous response)
curl -s http://localhost:8080/jobs/<id>

# 4. Live metrics
curl -s http://localhost:8080/metrics
```

In `/metrics` you'll see `jobs_enqueued_total` (idempotent duplicates don't count),
`jobs_processed_total` with a `result` label (`completed` or `failed`), `jobs_in_flight`,
and the `job_processing_duration_seconds` histogram.

Business validation errors (in `internal/application/enqueue_job.go`) → `400`:

```bash
# empty type → 400
curl -s -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"","payload":{},"idempotency_key":"demo-bad"}'

# negative max_attempts → 400 (0 = default 5)
curl -s -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"test.echo","payload":{},"idempotency_key":"demo-bad-2","max_attempts":-1}'
```

To run the integration test suite against an isolated database (`jobs_test`, port
`:5433`, `postgres_test` service) without touching the manual-use database, see the
"Integration tests (Postgres)" section — it uses `TEST_DATABASE_URL` (or its default) and
`scripts/setup_test_db.sh`.

### Graceful shutdown in action

Real sequence of the server processing a job when a shutdown signal arrives: you can see
the worker finish the job in progress before exiting, instead of cutting it off mid-way.

```
2026/07/31 17:29:19 INFO go-durable-jobs starting
2026/07/31 17:29:19 INFO worker pool started num_workers=5
2026/07/31 17:29:24 INFO echo handler processing job type=test.echo payload={}
2026/07/31 17:29:24 INFO echo handler simulating slow work (5s)
2026/07/31 17:29:26 INFO signal received, starting graceful shutdown signal=interrupt
2026/07/31 17:29:29 INFO echo handler finished work
2026/07/31 17:29:29 INFO worker: job completed worker_id=2 job_id=62b6c374-abfc-47cf-a3b6-f52d1d4eed49
2026/07/31 17:29:29 INFO shutdown complete
```

The signal (SIGINT) arrived at 17:29:26, in the middle of the simulated 5s sleep that
started at 17:29:24. The process doesn't cut the job off: it waits for it to finish on its
own (17:29:29) before logging `shutdown complete` and exiting. Note: this log comes from a
manual test with a temporary sleep added to the handler to force that 5s window — it's not
the normal behavior of `echoHandler`, which processes instantly.

### Integration tests (Postgres)

Integration tests run against a **separate** database (`jobs_test`, `postgres_test`
service in `docker-compose.yml`, host port `5433`), never against the manual-use database
(`jobs`, port `5432`). This guarantees that a test that forgets to clean up never mixes
data with your manual `curl` testing.

Setup (once per machine):

```bash
docker compose up -d postgres_test
scripts/setup_test_db.sh
```

This script applies the raw SQL (`migrations/0001_create_jobs_table.up.sql`) with
`psql -f` purely for local simplicity; a real deployment flow would use the
`golang-migrate` CLI, which tracks versions and doesn't require the SQL to be idempotent.

Resolution chain for the test connection (`TEST_DATABASE_URL` → `DATABASE_URL` → default):

```bash
# Recommended: isolated database for tests (jobs_test)
TEST_DATABASE_URL="postgres://jobs:jobs@localhost:5433/jobs_test?sslmode=disable" \
  go test -tags integration ./internal/infrastructure/postgres/

# Backward compatibility: if TEST_DATABASE_URL isn't set, DATABASE_URL is used.
# If neither is set, the default is used (jobs_test on 5433).
```

Each integration test truncates the `jobs` table at the start of its run (`truncateJobs`
helper), so none of them depend on another and none leave junk behind.

If you haven't started the `postgres_test` service, the tests **skip** (`t.Skip`) with a
message indicating what's missing, instead of failing hard.

## Benchmarks

Integration benchmarks against real Postgres, using the same isolated setup as the tests
(`jobs_test` on the `postgres_test` service). They live in
`internal/infrastructure/postgres/postgres_job_repository_bench_test.go` and use the same
connection chain (`TEST_DATABASE_URL` → `DATABASE_URL` → default).

Measured on: Ryzen 3 PRO 2200G (4 physical cores, no SMT), Windows, Postgres 16 in Docker.
The numbers are **indicative of this environment**, not a general ceiling — running them
on different hardware may vary significantly.

### How to run

```bash
go test -tags integration -bench='Benchmark' -run='^$' -benchmem -benchtime=1000x \
  -count=5 ./internal/infrastructure/postgres/
```

Execution notes:

- **`-benchtime=Nx` (fixed iterations), not time**: each benchmark truncates and reseeds
  data on every invocation; with `-benchtime=5s` the harness recalibrates `b.N` across
  several rounds, multiplying the seeding cost without adding precision. With `Nx`, total
  cost stays bounded and predictable.
- **PowerShell**: flags containing commas need to be quoted — `'-cpu=1,2,4,8'` — because
  the comma gets parsed as an argument separator and the command fails.

### Results

1000 ops per run. `-count=5` (Create and Dequeue), `-count=3` (ConcurrentDequeue).
Reported values: median with range (min–max) across runs. `jobs_s` = operations per
second.

| Benchmark | jobs_s (median) | range | p50 | p99 | allocs/op |
|---|---|---|---|---|---|
| Create | 298.5 | 210–362 | 2.7ms | 15ms | 38 |
| Dequeue (Dequeue+MarkCompleted) | 100.2 | 74–106 | 8.8ms | 31ms | 94 |
| ConcurrentDequeue (4 workers) | 231 | 231–239 | – | – | 70 |

The **variance across runs** comes from the tail, not the median: `p50` is stable in every
run (Create 2.5–3.0ms, Dequeue 8.7–11.0ms), but a handful of ops with 20–50ms latency skew
the mean. The most likely cause —a reasoned hypothesis, **not verified**: `iostat` and
`GODEBUG=gctrace=1` were not run to confirm it— is WAL `fsync` on every `COMMIT` (each
operation is a commit) under Docker-on-Windows, which has highly variable fsync latencies.
That's why `ns/op` (the mean) fluctuates between runs while `p50` doesn't; this README
reports median and p50 as the stable metrics.

### `SKIP LOCKED` scaling under concurrency

`BenchmarkConcurrentDequeue`, 1000 ops, `-count=3`, varying the number of workers with
`-cpu`:

```bash
go test -tags integration -bench='BenchmarkConcurrentDequeue' -run='^$' -benchmem \
  -benchtime=1000x '-cpu=1,2,4,8' -count=3 ./internal/infrastructure/postgres/
```

| `-cpu` (workers) | jobs_s (median) |
|---|---|
| 1 | 143 |
| 2 | 191 (+33%) |
| 4 | 231 (+21%) |
| 8 | 229 (≈0%) |

Conclusion: **no sign of degradation from lock contention**. Throughput grows up to ~4
workers and flattens out — the ceiling is the hardware (4 physical cores), not the dequeue
mechanism. `-cpu=8` oversubscribes the 4 real cores and gains nothing. This is the
expected behavior of a well-designed dequeue: the saturation point is Postgres itself (if
the `COMMIT`-fsync hypothesis is correct), not contention on `SKIP LOCKED`.

### Methodology note

Before trusting the numbers, we checked the benchmarking harness itself and found two
issues:

1. **`b.ResetTimer()` doesn't restart a timer stopped by `StopTimer()`** — it only stops
   it and zeroes the accumulated time; you need to call `b.StartTimer()` afterward. Our
   first run reported `+Inf jobs_s` and no `ns/op` for exactly this reason. The correct
   pattern is `StopTimer → setup → StartTimer → loop → StopTimer`.
2. **Comma escaping in PowerShell** — `-cpu=1,2,4,8` without quotes gets parsed as an
   argument array and the command fails. It needs to be quoted: `'-cpu=1,2,4,8'`.

The first result that came out (a single run, with the timer misused) is **not** what
appears in this README. The numbers above come from `-count=3`/`-count=5` runs with the
corrected pattern.

## Metrics

Exposed at `GET /metrics` in Prometheus format (`internal/telemetry/metrics.go`):

| Metric | Type | What it measures |
|---|---|---|
| `jobs_enqueued_total` | Counter | New jobs enqueued (idempotent duplicates don't count) |
| `jobs_processed_total{result}` | Counter | Processing attempts, by outcome (`completed`/`failed`) — `failed` includes both business-logic failures in the handler and later persistence failures (`MarkCompleted`/`MarkFailed`) |
| `jobs_in_flight` | Gauge | Jobs currently being processed |
| `job_processing_duration_seconds` | Histogram | Duration of each attempt (handler + `Mark*`), custom buckets up to 300s |

Comparing `jobs_enqueued_total` against the sum of `jobs_processed_total` is the simplest
way to spot "lost" jobs (enqueued but never processed) at a glance.

## What I'd improve today (honest retrospective)

This is a portfolio project that solves a real problem with a solid architecture, but it's
not a production system. This section makes explicit what was deliberately left out and
why, what I'd do differently with more time, and what worked better than expected — the
same standard of honesty as the benchmarks section.

### Deliberate decisions not to implement

**Circuit Breaker.** MVP jobs are generic and don't call external services by default:
there's nothing downstream to protect from failure, and a circuit breaker with no external
dependencies just adds complexity. If a job type that does call external APIs gets added
in the future, the circuit breaker would live in that job's business handler (per
host/API, with a failure threshold and cooldown before opening), never in the
dequeue/enqueue machinery, which isn't the layer that fails in that scenario. (Full detail
in `DECISIONS.md`, section 2.4.)

**Redis.** Left for Phase 2 because Postgres's `idempotency_key` unique constraint already
covers MVP idempotency atomically and durably, with no extra piece to operate. Redis would
add an ultra-fast idempotency check (a read instead of a full round trip) and rate
limiting — valuable for a high-volume producer, not critical at the current scale.

### What I'd do differently with more time

- **Benchmark variance isn't fully resolved.** The `fsync` hypothesis under
  Docker-on-Windows is documented, not confirmed. With more time I'd run the suite on
  native Linux or with `iostat` / `GODEBUG=gctrace=1` to rule the real cause in or out.
- **`echoHandler` is a placeholder.** The pool receives a single hardcoded handler in
  `main.go` that logs and returns `nil`. A real system would need a registry mapping
  `type` → business handler; the interface is already there
  (`Handle(ctx, jobType, payload)`), the dispatch piece is what's missing.
- **The HTTP API has no authentication or authorization.** Anyone who reaches the port can
  enqueue and requeue jobs. Acceptable for a local portfolio; a non-starter as-is in
  production.
- **Distributed tracing (OpenTelemetry) was left for Phase 2.** Makes sense if the system
  grows into multiple services; for a single process it's over-engineering.
- **The inspection/requeue CLI was never built.** It was in the original plan and got
  dropped: today the only way to requeue is `POST /jobs/{id}/requeue`, and there's no way
  to list all `dead` jobs in one pass. In practice, it would be the first Phase 2 addition.

### What worked well (and why)

The layer-by-layer review process, with real fixes before moving forward, was what
prevented the most bugs — at least four would have been hard to debug after the fact:

- the original race condition between Dequeue and MarkRunning (two workers could pick up
  the same job);
- a bug in MarkFailed left jobs with retries pending in a state (`failed`) that Dequeue
  never looks at again — retries were effectively broken even though the backoff was
  computed correctly. It's the most dangerous kind of bug: nothing breaks immediately, it
  shows up as "retries just don't happen" much later;
- the asymmetric return values in ProcessJob (success and failure weren't reported
  consistently);
- cross-contamination between the integration test database and the manual-use database:
  leftover jobs from tests showed up as processed in the live metrics. The system was
  working correctly, but the numbers were saying something that wasn't quite what it
  looked like; it was caught by checking instead of assuming, and led to physically
  isolating the databases (`jobs_test` on port 5433).

Catching them during review, when each layer was small and the context was fresh, took
minutes; catching them later, with workers already running, would have taken hours and a
much more painful debugging session.
