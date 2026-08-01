package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

type EnqueueJobRequest struct {
	Type           string
	Payload        json.RawMessage
	IdempotencyKey string
	Priority       domain.Priority
	Delay          time.Duration
	MaxAttempts    int
}

type EnqueueJobResult struct {
	Job            *domain.Job
	AlreadyExisted bool
}

type EnqueueJob struct {
	repo    domain.JobRepository
	metrics MetricsRecorder
}

func NewEnqueueJob(repo domain.JobRepository, metrics MetricsRecorder) *EnqueueJob {
	// El chequeo nil solo cubre nil literal (interfaz vacía), no un puntero
	// concreto nulo envuelto en la interfaz — ver "typed nil interface" en Go.
	// main.go siempre pasa una instancia real de telemetry.Metrics.
	if metrics == nil {
		metrics = noopMetricsRecorder{}
	}
	return &EnqueueJob{repo: repo, metrics: metrics}
}

func (uc *EnqueueJob) Execute(ctx context.Context, req EnqueueJobRequest) (*EnqueueJobResult, error) {
	if req.Type == "" {
		return nil, fmt.Errorf("%w: type is required", ErrInvalidRequest)
	}
	if req.IdempotencyKey == "" {
		return nil, fmt.Errorf("%w: idempotency key is required", ErrInvalidRequest)
	}
	if req.Payload == nil {
		return nil, fmt.Errorf("%w: payload is required", ErrInvalidRequest)
	}

	if req.MaxAttempts < 0 {
		return nil, fmt.Errorf("%w: max_attempts must be >= 1 when provided", ErrInvalidRequest)
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = 5
	}

	job := &domain.Job{
		ID:             uuid.New(),
		IdempotencyKey: req.IdempotencyKey,
		Type:           req.Type,
		Payload:        req.Payload,
		Status:         domain.StatusPending,
		Priority:       req.Priority,
		Attempts:       0,
		MaxAttempts:    req.MaxAttempts,
		AvailableAt:    time.Now().Add(req.Delay),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	err := uc.repo.Create(ctx, job)
	if err != nil {
		if errors.Is(err, domain.ErrDuplicateIdempotencyKey) {
			existing, findErr := uc.repo.FindByIdempotencyKey(ctx, req.IdempotencyKey)
			if findErr != nil {
				return nil, fmt.Errorf("find after duplicate: %w", findErr)
			}
			return &EnqueueJobResult{Job: existing, AlreadyExisted: true}, nil
		}
		return nil, fmt.Errorf("create: %w", err)
	}

	uc.metrics.IncJobsEnqueued()
	return &EnqueueJobResult{Job: job, AlreadyExisted: false}, nil
}
