//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

const defaultDatabaseURL = "postgres://jobs:jobs@localhost:5433/jobs_test?sslmode=disable"

func integrationDB(t testing.TB) *pgxpool.Pool {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}
	if databaseURL == "" {
		databaseURL = defaultDatabaseURL
	}

	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Skipf("could not connect to postgres (%s): %v — levantá el servicio postgres_test (docker compose up -d postgres_test) y aplicá la migración (scripts/setup_test_db.sh)", databaseURL, err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func truncateJobs(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), "TRUNCATE TABLE jobs")
	if err != nil {
		t.Fatalf("truncate jobs: %v", err)
	}
}

func TestPostgresJobRepository_HappyPath(t *testing.T) {
	pool := integrationDB(t)
	repo := NewPostgresJobRepository(pool)
	ctx := context.Background()

	truncateJobs(t, pool)

	idempotencyKey := uuid.New().String()

	// a) Create a new job
	job := &domain.Job{
		ID:             uuid.New(),
		IdempotencyKey: idempotencyKey,
		Type:           "test",
		Payload:        json.RawMessage(`{"msg":"hello"}`),
		Status:         domain.StatusPending,
		Priority:       0,
		Attempts:       0,
		MaxAttempts:    5,
		AvailableAt:    time.Now().Add(-1 * time.Hour),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if err := repo.Create(ctx, job); err != nil {
		t.Fatalf("create: %v", err)
	}

	// b) Duplicate idempotency key → ErrDuplicateIdempotencyKey
	dup := *job
	dup.ID = uuid.New()
	if err := repo.Create(ctx, &dup); err != domain.ErrDuplicateIdempotencyKey {
		t.Fatalf("expected ErrDuplicateIdempotencyKey, got %v", err)
	}

	// c) FindByIdempotencyKey → returns the created job
	found, err := repo.FindByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if found.ID != job.ID {
		t.Fatalf("expected id %v, got %v", job.ID, found.ID)
	}

	// d) Dequeue → status=running, started_at set
	dequeued, err := repo.Dequeue(ctx)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if dequeued.ID != job.ID {
		t.Fatalf("expected job id %v, got %v", job.ID, dequeued.ID)
	}
	if dequeued.Status != domain.StatusRunning {
		t.Fatalf("expected status running, got %v", dequeued.Status)
	}
	if dequeued.StartedAt == nil || dequeued.StartedAt.IsZero() {
		t.Fatal("expected started_at to be set")
	}

	// e) Dequeue again (no other jobs) → ErrNoJobsAvailable
	_, err = repo.Dequeue(ctx)
	if err != domain.ErrNoJobsAvailable {
		t.Fatalf("expected ErrNoJobsAvailable, got %v", err)
	}

	// f) MarkFailed with nextAvailableAt 2s in the future
	nextAvailableAt := time.Now().Add(2 * time.Second)
	if err := repo.MarkFailed(ctx, dequeued.ID, "test error", nextAvailableAt); err != nil {
		t.Fatalf("mark failed: %v", err)
	}

	afterFail, err := repo.FindByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		t.Fatalf("find after fail: %v", err)
	}
	if afterFail.Status != domain.StatusPending {
		t.Fatalf("expected status pending after MarkFailed, got %v", afterFail.Status)
	}
	if afterFail.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", afterFail.Attempts)
	}
	if afterFail.LastError == nil || *afterFail.LastError != "test error" {
		t.Fatalf("expected last_error='test error', got %v", afterFail.LastError)
	}

	// Dequeue should still return ErrNoJobsAvailable (available_at in future)
	_, err = repo.Dequeue(ctx)
	if err != domain.ErrNoJobsAvailable {
		t.Fatalf("expected ErrNoJobsAvailable before available_at, got %v", err)
	}

	// g) Wait for available_at to pass, then dequeue again
	time.Sleep(3 * time.Second)

	secondDequeue, err := repo.Dequeue(ctx)
	if err != nil {
		t.Fatalf("second dequeue: %v", err)
	}
	if secondDequeue.ID != job.ID {
		t.Fatalf("expected job id %v, got %v", job.ID, secondDequeue.ID)
	}
	if secondDequeue.Attempts != 1 {
		t.Fatalf("expected attempts=1 on second attempt, got %d", secondDequeue.Attempts)
	}

	// h) MarkCompleted
	if err := repo.MarkCompleted(ctx, secondDequeue.ID, time.Now()); err != nil {
		t.Fatalf("mark completed: %v", err)
	}

	// i) Confirm status=completed
	completed, err := repo.FindByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		t.Fatalf("find after complete: %v", err)
	}
	if completed.Status != domain.StatusCompleted {
		t.Fatalf("expected status completed, got %v", completed.Status)
	}
	if completed.CompletedAt == nil || completed.CompletedAt.IsZero() {
		t.Fatal("expected completed_at to be set")
	}
}

func TestPostgresJobRepository_FindByID(t *testing.T) {
	pool := integrationDB(t)
	repo := NewPostgresJobRepository(pool)
	ctx := context.Background()

	truncateJobs(t, pool)

	idempotencyKey := uuid.New().String()
	job := &domain.Job{
		ID:             uuid.New(),
		IdempotencyKey: idempotencyKey,
		Type:           "test",
		Payload:        json.RawMessage(`{"msg":"hi"}`),
		Status:         domain.StatusPending,
		Priority:       0,
		Attempts:       0,
		MaxAttempts:    5,
		AvailableAt:    time.Now(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	if err := repo.Create(ctx, job); err != nil {
		t.Fatalf("create: %v", err)
	}

	found, err := repo.FindByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("find by id: %v", err)
	}
	if found.ID != job.ID || found.IdempotencyKey != idempotencyKey {
		t.Fatalf("unexpected job: %+v", found)
	}

	if _, err := repo.FindByID(ctx, uuid.New()); err != domain.ErrJobNotFound {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestPostgresJobRepository_Requeue(t *testing.T) {
	pool := integrationDB(t)
	repo := NewPostgresJobRepository(pool)
	ctx := context.Background()

	truncateJobs(t, pool)

	makeJob := func(status domain.JobStatus, attempts, maxAttempts int) *domain.Job {
		job := &domain.Job{
			ID:             uuid.New(),
			IdempotencyKey: uuid.New().String(),
			Type:           "test",
			Payload:        json.RawMessage(`{}`),
			Status:         status,
			Priority:       0,
			Attempts:       attempts,
			MaxAttempts:    maxAttempts,
			AvailableAt:    time.Now(),
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		}
		if err := repo.Create(ctx, job); err != nil {
			t.Fatalf("create: %v", err)
		}
		return job
	}

	// a) Dead job → requeued to pending with attempts reset
	dead := makeJob(domain.StatusDead, 5, 5)
	if err := repo.Requeue(ctx, dead.ID); err != nil {
		t.Fatalf("requeue dead: %v", err)
	}
	after, err := repo.FindByID(ctx, dead.ID)
	if err != nil {
		t.Fatalf("find after requeue: %v", err)
	}
	if after.Status != domain.StatusPending {
		t.Fatalf("expected status pending after requeue, got %v", after.Status)
	}
	if after.Attempts != 0 {
		t.Fatalf("expected attempts=0 after requeue, got %d", after.Attempts)
	}
	if after.LastError != nil {
		t.Fatalf("expected last_error=NULL after requeue, got %v", *after.LastError)
	}

	// b) Existing job not dead → ErrJobNotDead
	running := makeJob(domain.StatusRunning, 1, 5)
	if err := repo.Requeue(ctx, running.ID); err != domain.ErrJobNotDead {
		t.Fatalf("expected ErrJobNotDead, got %v", err)
	}

	// c) Nonexistent id → ErrJobNotFound
	if err := repo.Requeue(ctx, uuid.New()); err != domain.ErrJobNotFound {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}
