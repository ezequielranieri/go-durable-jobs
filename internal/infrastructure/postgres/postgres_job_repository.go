package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

type PostgresJobRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresJobRepository(pool *pgxpool.Pool) *PostgresJobRepository {
	return &PostgresJobRepository{pool: pool}
}

func mapPgError(err error) error {
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "idx_jobs_idempotency_key" {
		return domain.ErrDuplicateIdempotencyKey
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrJobNotFound
	}

	return fmt.Errorf("postgres: %w", err)
}

func scanJob(row pgx.Row) (*domain.Job, error) {
	var job domain.Job
	var status string
	err := row.Scan(
		&job.ID, &job.IdempotencyKey, &job.Type, &job.Payload,
		&status, &job.Priority, &job.Attempts, &job.MaxAttempts,
		&job.AvailableAt, &job.StartedAt, &job.CompletedAt,
		&job.LastError, &job.CreatedAt, &job.UpdatedAt,
	)
	if err != nil {
		return nil, mapPgError(err)
	}
	job.Status = domain.JobStatus(status)
	return &job, nil
}

func (r *PostgresJobRepository) Create(ctx context.Context, job *domain.Job) error {
	query := `
		INSERT INTO jobs (id, idempotency_key, type, payload, status, priority, attempts, max_attempts, available_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	_, err := r.pool.Exec(ctx, query,
		job.ID, job.IdempotencyKey, job.Type, job.Payload,
		job.Status, job.Priority, job.Attempts, job.MaxAttempts,
		job.AvailableAt, job.CreatedAt, job.UpdatedAt,
	)
	if err != nil {
		return mapPgError(err)
	}
	return nil
}

func (r *PostgresJobRepository) FindByIdempotencyKey(ctx context.Context, key string) (*domain.Job, error) {
	query := `
		SELECT id, idempotency_key, type, payload, status, priority, attempts, max_attempts,
		       available_at, started_at, completed_at, last_error, created_at, updated_at
		FROM jobs
		WHERE idempotency_key = $1
	`
	row := r.pool.QueryRow(ctx, query, key)
	return scanJob(row)
}

func (r *PostgresJobRepository) FindByID(ctx context.Context, id uuid.UUID) (*domain.Job, error) {
	query := `
		SELECT id, idempotency_key, type, payload, status, priority, attempts, max_attempts,
		       available_at, started_at, completed_at, last_error, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`
	row := r.pool.QueryRow(ctx, query, id)
	return scanJob(row)
}

func (r *PostgresJobRepository) Dequeue(ctx context.Context) (*domain.Job, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	selectQuery := `
		SELECT id, idempotency_key, type, payload, status, priority, attempts, max_attempts,
		       available_at, started_at, completed_at, last_error, created_at, updated_at
		FROM jobs
		WHERE status = 'pending' AND available_at <= NOW()
		ORDER BY priority DESC, created_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`
	row := tx.QueryRow(ctx, selectQuery)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			return nil, domain.ErrNoJobsAvailable
		}
		return nil, fmt.Errorf("postgres: dequeue select: %w", err)
	}

	updateQuery := `
		UPDATE jobs SET status = 'running', started_at = NOW(), updated_at = NOW()
		WHERE id = $1
		RETURNING started_at, updated_at
	`
	var startedAt time.Time
	var updatedAt time.Time
	if err := tx.QueryRow(ctx, updateQuery, job.ID).Scan(&startedAt, &updatedAt); err != nil {
		return nil, fmt.Errorf("postgres: dequeue update: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: dequeue commit: %w", err)
	}

	job.Status = domain.StatusRunning
	job.StartedAt = &startedAt
	job.UpdatedAt = updatedAt

	return job, nil
}

func (r *PostgresJobRepository) MarkCompleted(ctx context.Context, id uuid.UUID, completedAt time.Time) error {
	query := `
		UPDATE jobs SET status = 'completed', completed_at = $2, updated_at = NOW()
		WHERE id = $1 AND status = 'running'
	`
	ct, err := r.pool.Exec(ctx, query, id, completedAt)
	if err != nil {
		return fmt.Errorf("postgres: mark completed: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return domain.ErrInvalidStatusTransition
	}
	return nil
}

func (r *PostgresJobRepository) MarkFailed(ctx context.Context, id uuid.UUID, lastError string, nextAvailableAt time.Time) error {
	query := `
		UPDATE jobs SET
		    attempts = attempts + 1,
		    last_error = $2,
		    status = CASE WHEN attempts + 1 >= max_attempts THEN 'dead' ELSE 'pending' END,
		    available_at = CASE WHEN attempts + 1 >= max_attempts THEN available_at ELSE $3 END,
		    updated_at = NOW()
		WHERE id = $1 AND status = 'running'
	`
	ct, err := r.pool.Exec(ctx, query, id, lastError, nextAvailableAt)
	if err != nil {
		return fmt.Errorf("postgres: mark failed: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return domain.ErrInvalidStatusTransition
	}
	return nil
}

// Requeue resets a dead job back to pending.
// It distinguishes "job does not exist" from "job exists but is not dead":
//   - nil if the job was dead and has been requeued
//   - domain.ErrJobNotFound if the id does not exist
//   - domain.ErrJobNotDead if the job exists but is not in 'dead' status
func (r *PostgresJobRepository) Requeue(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE jobs SET status = 'pending', attempts = 0, last_error = NULL, available_at = NOW(), updated_at = NOW()
		WHERE id = $1 AND status = 'dead'
	`
	ct, err := r.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("postgres: requeue: %w", err)
	}
	if ct.RowsAffected() == 0 {
		var exists bool
		if err := r.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM jobs WHERE id = $1)", id).Scan(&exists); err != nil {
			return fmt.Errorf("postgres: requeue existence check: %w", err)
		}
		if !exists {
			return domain.ErrJobNotFound
		}
		return domain.ErrJobNotDead
	}
	return nil
}
