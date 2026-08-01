package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type JobRepository interface {
	Create(ctx context.Context, job *Job) error

	FindByIdempotencyKey(ctx context.Context, key string) (*Job, error)

	FindByID(ctx context.Context, id uuid.UUID) (*Job, error)

	Dequeue(ctx context.Context) (*Job, error)

	MarkCompleted(ctx context.Context, id uuid.UUID, completedAt time.Time) error

	// MarkFailed increments attempts and sets last_error.
	// If attempts >= max_attempts → status = 'dead'.
	// Otherwise → status = 'pending', available_at = nextAvailableAt.
	//
	// nextAvailableAt is computed by the application layer as
	// time.Now().Add(backoffDuration) using the Job.Attempts returned by Dequeue.
	// It MUST NOT re-read the Job from the database to calculate it, since
	// another worker could have dequeued it in between. This invariant relies on
	// Dequeue holding the job in 'running' exclusively (via transaction +
	// FOR UPDATE SKIP LOCKED). If Dequeue's locking semantics ever change,
	// this contract must be revisited.
	MarkFailed(ctx context.Context, id uuid.UUID, lastError string, nextAvailableAt time.Time) error

	Requeue(ctx context.Context, id uuid.UUID) error
}
