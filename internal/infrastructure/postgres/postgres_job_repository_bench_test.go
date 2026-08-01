//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

// seedPendingJobs inserts n pending jobs via a single COPY batch. Setup cost
// is not part of the measurement (callers stop the timer around it). The
// payload is passed as a plain string: passing []byte would make pgx encode it
// as bytea, which fails against the jsonb column.
func seedPendingJobs(b testing.TB, pool *pgxpool.Pool, n int) {
	b.Helper()
	if n <= 0 {
		return
	}

	columns := []string{
		"id", "idempotency_key", "type", "payload", "status",
		"priority", "attempts", "max_attempts", "available_at",
		"created_at", "updated_at",
	}
	rows := pgx.CopyFromSlice(n, func(i int) ([]any, error) {
		now := time.Now()
		return []any{
			uuid.New(),
			fmt.Sprintf("bench_%d", i),
			"bench",
			"{}",
			domain.StatusPending,
			0, 0, 5,
			now.Add(-1 * time.Hour),
			now,
			now,
		}, nil
	})

	if _, err := pool.CopyFrom(context.Background(), pgx.Identifier{"jobs"}, columns, rows); err != nil {
		b.Fatalf("seed %d pending jobs: %v", n, err)
	}
}

func newBenchJob(i int) *domain.Job {
	now := time.Now()
	return &domain.Job{
		ID:             uuid.New(),
		IdempotencyKey: fmt.Sprintf("bench_%d", i),
		Type:           "bench",
		Payload:        json.RawMessage(`{}`),
		Status:         domain.StatusPending,
		Priority:       0,
		Attempts:       0,
		MaxAttempts:    5,
		AvailableAt:    now,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func reportPercentiles(b *testing.B, durations []time.Duration) {
	if len(durations) == 0 {
		return
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	p50 := durations[int(float64(len(durations))*0.50)]
	p99 := durations[int(float64(len(durations))*0.99)]
	b.ReportMetric(float64(p50)/float64(time.Microsecond), "p50_us")
	b.ReportMetric(float64(p99)/float64(time.Microsecond), "p99_us")
}

// BenchmarkCreate measures single-insert throughput with unique idempotency
// keys. The table grows up to b.N rows during the run, so index maintenance
// cost is realistic.
func BenchmarkCreate(b *testing.B) {
	pool := integrationDB(b)
	repo := NewPostgresJobRepository(pool)
	ctx := context.Background()

	b.StopTimer()
	truncateJobs(b, pool)
	durations := make([]time.Duration, 0, b.N)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		if err := repo.Create(ctx, newBenchJob(i)); err != nil {
			b.Fatalf("create: %v", err)
		}
		durations = append(durations, time.Since(start))
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs_s")
	reportPercentiles(b, durations)
}

// BenchmarkDequeue measures Dequeue+MarkCompleted throughput. The table is
// pre-populated with b.N pending jobs so each iteration consumes exactly one.
func BenchmarkDequeue(b *testing.B) {
	pool := integrationDB(b)
	repo := NewPostgresJobRepository(pool)
	ctx := context.Background()

	b.StopTimer()
	truncateJobs(b, pool)
	seedPendingJobs(b, pool, b.N)
	durations := make([]time.Duration, 0, b.N)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		job, err := repo.Dequeue(ctx)
		if err != nil {
			b.Fatalf("dequeue: %v", err)
		}
		if err := repo.MarkCompleted(ctx, job.ID, time.Now()); err != nil {
			b.Fatalf("mark completed: %v", err)
		}
		durations = append(durations, time.Since(start))
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs_s")
	reportPercentiles(b, durations)
}

// BenchmarkConcurrentDequeue simulates several workers hitting Dequeue at the
// same time (SKIP LOCKED contention). Dequeue only, no MarkCompleted: the
// UPDATE in MarkCompleted does not contend, so it would dilute the hotspot we
// want to measure. b.Fatal is not allowed inside RunParallel callbacks, so
// errors are collected and asserted after.
func BenchmarkConcurrentDequeue(b *testing.B) {
	pool := integrationDB(b)
	repo := NewPostgresJobRepository(pool)
	ctx := context.Background()

	b.StopTimer()
	truncateJobs(b, pool)
	seedPendingJobs(b, pool, b.N)
	b.StartTimer()

	var (
		mu     sync.Mutex
		errCnt int
		errMsg string
	)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := repo.Dequeue(ctx); err != nil {
				mu.Lock()
				if errCnt == 0 {
					errMsg = err.Error()
				}
				errCnt++
				mu.Unlock()
			}
		}
	})
	b.StopTimer()

	mu.Lock()
	defer mu.Unlock()
	if errCnt > 0 {
		b.Fatalf("concurrent dequeue: %d errors, first: %s", errCnt, errMsg)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs_s")
}
