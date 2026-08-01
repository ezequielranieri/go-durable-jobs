package worker_test

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

type controlledRepo struct {
	mu             sync.Mutex
	jobs           []*domain.Job
	allJobs        map[uuid.UUID]*domain.Job
	dequeueCount   int
}

func (r *controlledRepo) DequeueCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dequeueCount
}

func (r *controlledRepo) Create(_ context.Context, _ *domain.Job) error {
	panic("unexpected call to Create")
}

func (r *controlledRepo) FindByIdempotencyKey(_ context.Context, _ string) (*domain.Job, error) {
	panic("unexpected call to FindByIdempotencyKey")
}

func (r *controlledRepo) FindByID(_ context.Context, _ uuid.UUID) (*domain.Job, error) {
	panic("unexpected call to FindByID")
}

func (r *controlledRepo) Dequeue(_ context.Context) (*domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dequeueCount++
	if len(r.jobs) == 0 {
		return nil, domain.ErrNoJobsAvailable
	}
	job := r.jobs[0]
	r.jobs = r.jobs[1:]
	if r.allJobs == nil {
		r.allJobs = make(map[uuid.UUID]*domain.Job)
	}
	job.Status = domain.StatusRunning
	now := time.Now()
	job.StartedAt = &now
	r.allJobs[job.ID] = job
	return job, nil
}

func (r *controlledRepo) MarkCompleted(_ context.Context, id uuid.UUID, completedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.allJobs[id]
	if !ok {
		return domain.ErrJobNotFound
	}
	job.Status = domain.StatusCompleted
	job.CompletedAt = &completedAt
	job.UpdatedAt = time.Now()
	return nil
}

func (r *controlledRepo) MarkFailed(_ context.Context, id uuid.UUID, lastError string, nextAvailableAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.allJobs[id]
	if !ok {
		return domain.ErrJobNotFound
	}
	job.Attempts++
	job.LastError = &lastError
	job.UpdatedAt = time.Now()
	if job.Attempts >= job.MaxAttempts {
		job.Status = domain.StatusDead
	} else {
		job.Status = domain.StatusPending
		job.AvailableAt = nextAvailableAt
	}
	return nil
}

func (r *controlledRepo) Requeue(_ context.Context, _ uuid.UUID) error {
	panic("unexpected call to Requeue")
}

type controllableHandler struct {
	workTime time.Duration
	result   error
	started  chan struct{}
	done     chan struct{}
}

func (h *controllableHandler) Handle(_ context.Context, _ string, _ json.RawMessage) error {
	close(h.started)
	<-time.After(h.workTime)
	close(h.done)
	return h.result
}
