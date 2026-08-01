package application_test

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

type fakeJobRepository struct {
	mu                sync.Mutex
	jobs              map[uuid.UUID]*domain.Job
	idempotencyKeys   map[string]uuid.UUID
	created           int
	createErr         error
	markFailedErr     error
	markCompletedErr  error

	lastFailedID            uuid.UUID
	lastFailedError         string
	lastFailedNextAvailable time.Time
}

func newFakeJobRepository() *fakeJobRepository {
	return &fakeJobRepository{
		jobs:            make(map[uuid.UUID]*domain.Job),
		idempotencyKeys: make(map[string]uuid.UUID),
	}
}

func (f *fakeJobRepository) Create(_ context.Context, job *domain.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return f.createErr
	}
	if _, ok := f.idempotencyKeys[job.IdempotencyKey]; ok {
		return domain.ErrDuplicateIdempotencyKey
	}
	cp := *job
	f.jobs[job.ID] = &cp
	f.idempotencyKeys[job.IdempotencyKey] = job.ID
	f.created++
	return nil
}

func (f *fakeJobRepository) Created() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.created
}

func (f *fakeJobRepository) FindByIdempotencyKey(_ context.Context, key string) (*domain.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.idempotencyKeys[key]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	job, ok := f.jobs[id]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	cp := *job
	return &cp, nil
}

func (f *fakeJobRepository) FindByID(_ context.Context, id uuid.UUID) (*domain.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	job, ok := f.jobs[id]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	cp := *job
	return &cp, nil
}

func (f *fakeJobRepository) Dequeue(_ context.Context) (*domain.Job, error) {
	return nil, domain.ErrNoJobsAvailable
}

func (f *fakeJobRepository) MarkCompleted(_ context.Context, id uuid.UUID, completedAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.markCompletedErr != nil {
		return f.markCompletedErr
	}
	job, ok := f.jobs[id]
	if !ok {
		return domain.ErrJobNotFound
	}
	if job.Status != domain.StatusRunning {
		return domain.ErrInvalidStatusTransition
	}
	job.Status = domain.StatusCompleted
	job.CompletedAt = &completedAt
	job.UpdatedAt = time.Now()
	return nil
}

func (f *fakeJobRepository) MarkFailed(_ context.Context, id uuid.UUID, lastError string, nextAvailableAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.markFailedErr != nil {
		return f.markFailedErr
	}
	job, ok := f.jobs[id]
	if !ok {
		return domain.ErrJobNotFound
	}
	if job.Status != domain.StatusRunning {
		return domain.ErrInvalidStatusTransition
	}
	job.Attempts++
	job.LastError = &lastError
	job.UpdatedAt = time.Now()

	f.lastFailedID = id
	f.lastFailedError = lastError
	f.lastFailedNextAvailable = nextAvailableAt

	if job.Attempts >= job.MaxAttempts {
		job.Status = domain.StatusDead
	} else {
		job.Status = domain.StatusPending
		job.AvailableAt = nextAvailableAt
	}
	return nil
}

func (f *fakeJobRepository) Requeue(_ context.Context, _ uuid.UUID) error {
	return nil
}

type fakeJobHandler struct {
	result   error
	panicMsg string
}

func (h *fakeJobHandler) Handle(_ context.Context, _ string, _ json.RawMessage) error {
	if h.panicMsg != "" {
		panic(h.panicMsg)
	}
	return h.result
}

func makeRunningJob(repo *fakeJobRepository, attempts int) *domain.Job {
	job := &domain.Job{
		ID:             uuid.New(),
		IdempotencyKey: uuid.New().String(),
		Type:           "test",
		Payload:        json.RawMessage(`{}`),
		Status:         domain.StatusRunning,
		Priority:       0,
		Attempts:       attempts,
		MaxAttempts:    5,
		AvailableAt:    time.Now(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	repo.mu.Lock()
	cp := *job
	repo.jobs[job.ID] = &cp
	repo.idempotencyKeys[job.IdempotencyKey] = job.ID
	repo.mu.Unlock()
	return job
}
