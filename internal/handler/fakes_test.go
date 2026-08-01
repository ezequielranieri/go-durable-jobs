package handler_test

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

type fakeRepo struct {
	mu        sync.Mutex
	jobs      map[uuid.UUID]*domain.Job
	byKey     map[string]uuid.UUID
	created   int
	createErr error
	findErr   error
	requeue   error
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		jobs:  make(map[uuid.UUID]*domain.Job),
		byKey: make(map[string]uuid.UUID),
	}
}

func (f *fakeRepo) createCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.created
}

func (f *fakeRepo) Create(_ context.Context, job *domain.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return f.createErr
	}
	if _, ok := f.byKey[job.IdempotencyKey]; ok {
		return domain.ErrDuplicateIdempotencyKey
	}
	f.created++
	cp := *job
	f.jobs[job.ID] = &cp
	f.byKey[job.IdempotencyKey] = job.ID
	return nil
}

func (f *fakeRepo) FindByIdempotencyKey(_ context.Context, key string) (*domain.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.byKey[key]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	cp := *f.jobs[id]
	return &cp, nil
}

func (f *fakeRepo) FindByID(_ context.Context, id uuid.UUID) (*domain.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.findErr != nil {
		return nil, f.findErr
	}
	job, ok := f.jobs[id]
	if !ok {
		return nil, domain.ErrJobNotFound
	}
	cp := *job
	return &cp, nil
}

func (f *fakeRepo) Dequeue(_ context.Context) (*domain.Job, error) {
	return nil, domain.ErrNoJobsAvailable
}

func (f *fakeRepo) MarkCompleted(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}

func (f *fakeRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string, _ time.Time) error {
	return nil
}

func (f *fakeRepo) Requeue(_ context.Context, _ uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requeue
}

func seedJob(repo *fakeRepo, job *domain.Job) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	cp := *job
	repo.jobs[job.ID] = &cp
	repo.byKey[job.IdempotencyKey] = job.ID
}

func newJob() *domain.Job {
	return &domain.Job{
		ID:             uuid.New(),
		IdempotencyKey: uuid.New().String(),
		Type:           "send_email",
		Payload:        json.RawMessage(`{"to":"user@example.com"}`),
		Status:         domain.StatusPending,
		Priority:       0,
		Attempts:       0,
		MaxAttempts:    5,
		AvailableAt:    time.Now(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
}
