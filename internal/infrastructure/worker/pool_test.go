package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ezequielranieri/go-durable-jobs/internal/application"
	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
	"github.com/ezequielranieri/go-durable-jobs/internal/infrastructure/worker"
)

func TestPool_CleanShutdown(t *testing.T) {
	repo := &controlledRepo{allJobs: make(map[uuid.UUID]*domain.Job)}
	handler := &controllableHandler{
		workTime: 100 * time.Millisecond,
		started:  make(chan struct{}),
		done:     make(chan struct{}),
	}

	jobID := uuid.New()
	repo.jobs = []*domain.Job{{
		ID:          jobID,
		Payload:     json.RawMessage(`{}`),
		Attempts:    0,
		MaxAttempts: 5,
	}}

	pool, err := worker.NewPool(worker.PoolConfig{
		Repo:         repo,
		ProcessJob:   application.NewProcessJob(repo, time.Second, nil),
		Handler:      handler,
		NumWorkers:   1,
		PollInterval: 50 * time.Millisecond,
		GracePeriod:  2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	pool.Start(ctx)

	<-handler.started

	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	repo.mu.Lock()
	job := repo.allJobs[jobID]
	repo.mu.Unlock()

	if job == nil {
		t.Fatal("job not found in repo")
	}
	if job.Status != domain.StatusCompleted {
		t.Errorf("expected status completed, got %v", job.Status)
	}
}

func TestPool_JobNotInterruptedByShutdown(t *testing.T) {
	repo := &controlledRepo{allJobs: make(map[uuid.UUID]*domain.Job)}
	handler := &controllableHandler{
		workTime: 500 * time.Millisecond,
		started:  make(chan struct{}),
		done:     make(chan struct{}),
	}

	jobID := uuid.New()
	repo.jobs = []*domain.Job{{
		ID:          jobID,
		Payload:     json.RawMessage(`{}`),
		Attempts:    0,
		MaxAttempts: 5,
	}}

	pool, err := worker.NewPool(worker.PoolConfig{
		Repo:         repo,
		ProcessJob:   application.NewProcessJob(repo, time.Second, nil),
		Handler:      handler,
		NumWorkers:   1,
		PollInterval: 50 * time.Millisecond,
		GracePeriod:  2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	pool.Start(ctx)

	<-handler.started

	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	select {
	case <-handler.done:
	default:
		t.Fatal("shutdown returned before job completed")
	}

	repo.mu.Lock()
	job := repo.allJobs[jobID]
	repo.mu.Unlock()

	if job.Status != domain.StatusCompleted {
		t.Errorf("expected status completed, got %v", job.Status)
	}
}

func TestPool_GracePeriodExpired(t *testing.T) {
	repo := &controlledRepo{allJobs: make(map[uuid.UUID]*domain.Job)}
	handler := &controllableHandler{
		workTime: 2 * time.Second,
		started:  make(chan struct{}),
		done:     make(chan struct{}),
	}

	jobID := uuid.New()
	repo.jobs = []*domain.Job{{
		ID:          jobID,
		Payload:     json.RawMessage(`{}`),
		Attempts:    0,
		MaxAttempts: 5,
	}}

	pool, err := worker.NewPool(worker.PoolConfig{
		Repo:         repo,
		ProcessJob:   application.NewProcessJob(repo, time.Second, nil),
		Handler:      handler,
		NumWorkers:   1,
		PollInterval: 50 * time.Millisecond,
		GracePeriod:  200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	pool.Start(ctx)

	<-handler.started

	err = pool.Shutdown(context.Background())
	if !errors.Is(err, worker.ErrGracePeriodExpired) {
		t.Fatalf("expected ErrGracePeriodExpired, got %v", err)
	}

	select {
	case <-handler.done:
		t.Fatal("handler finished before grace period expired")
	default:
	}

	select {
	case <-handler.done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not finish after shutdown")
	}
}

func TestPool_PollingIntervalRespected(t *testing.T) {
	repo := &controlledRepo{}
	handler := &controllableHandler{
		started: make(chan struct{}),
		done:    make(chan struct{}),
	}

	pool, err := worker.NewPool(worker.PoolConfig{
		Repo:         repo,
		ProcessJob:   application.NewProcessJob(repo, time.Second, nil),
		Handler:      handler,
		NumWorkers:   1,
		PollInterval: 50 * time.Millisecond,
		GracePeriod:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	pool.Start(ctx)

	time.Sleep(200 * time.Millisecond)

	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	count := repo.DequeueCount()
	if count < 2 || count > 10 {
		t.Errorf("expected 2-10 Dequeue calls in 200ms with 50ms poll, got %d", count)
	}
}

func TestPool_ShutdownWithoutStart(t *testing.T) {
	repo := &controlledRepo{}
	handler := &controllableHandler{
		started: make(chan struct{}),
		done:    make(chan struct{}),
	}

	pool, err := worker.NewPool(worker.PoolConfig{
		Repo:         repo,
		ProcessJob:   application.NewProcessJob(repo, time.Second, nil),
		Handler:      handler,
		NumWorkers:   1,
		PollInterval: 50 * time.Millisecond,
		GracePeriod:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = pool.Shutdown(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "shutdown called before start") {
		t.Errorf("expected 'shutdown called before start', got %q", err.Error())
	}
}
