package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

type JobHandler interface {
	Handle(ctx context.Context, jobType string, payload json.RawMessage) error
}

type ProcessJob struct {
	repo      domain.JobRepository
	baseDelay time.Duration
	metrics   MetricsRecorder
}

func NewProcessJob(repo domain.JobRepository, baseDelay time.Duration, metrics MetricsRecorder) *ProcessJob {
	// El chequeo nil solo cubre nil literal (interfaz vacía), no un puntero
	// concreto nulo envuelto en la interfaz — ver "typed nil interface" en Go.
	// main.go siempre pasa una instancia real de telemetry.Metrics.
	if metrics == nil {
		metrics = noopMetricsRecorder{}
	}
	return &ProcessJob{repo: repo, baseDelay: baseDelay, metrics: metrics}
}

func (uc *ProcessJob) Execute(ctx context.Context, job *domain.Job, handler JobHandler) (err error) {
	uc.metrics.IncJobsInFlight()
	start := time.Now()
	defer func() {
		result := JobCompleted
		if err != nil {
			result = JobFailed
		}
		uc.metrics.IncJobsProcessed(result)
		uc.metrics.DecJobsInFlight()
		uc.metrics.ObserveJobProcessingDuration(time.Since(start))
	}()

	defer func() {
		if r := recover(); r != nil {
			panicErr := fmt.Errorf("panic in job handler: %v", r)
			backoff := domain.NextBackoff(job.Attempts+1, uc.baseDelay)
			nextAvailableAt := time.Now().Add(backoff)
			if markErr := uc.repo.MarkFailed(ctx, job.ID, panicErr.Error(), nextAvailableAt); markErr != nil {
				err = fmt.Errorf("%w (also mark failed: %v)", panicErr, markErr)
				return
			}
			err = panicErr
		}
	}()

	if err := handler.Handle(ctx, job.Type, job.Payload); err != nil {
		backoff := domain.NextBackoff(job.Attempts+1, uc.baseDelay)
		nextAvailableAt := time.Now().Add(backoff)
		if markErr := uc.repo.MarkFailed(ctx, job.ID, err.Error(), nextAvailableAt); markErr != nil {
			return fmt.Errorf("job error %w (also mark failed: %v)", err, markErr)
		}
		return fmt.Errorf("job failed: %w", err)
	}

	if err := uc.repo.MarkCompleted(ctx, job.ID, time.Now()); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	return nil
}
