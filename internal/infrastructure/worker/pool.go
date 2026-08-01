package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ezequielranieri/go-durable-jobs/internal/application"
	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

var ErrGracePeriodExpired = errors.New("grace period expired, some workers still running")

type PoolConfig struct {
	Repo         domain.JobRepository
	ProcessJob   *application.ProcessJob
	Handler      application.JobHandler
	NumWorkers   int
	PollInterval time.Duration
	GracePeriod  time.Duration
}

func (c PoolConfig) validate() error {
	if c.Repo == nil {
		return errors.New("repo is required")
	}
	if c.ProcessJob == nil {
		return errors.New("process job is required")
	}
	if c.Handler == nil {
		return errors.New("handler is required")
	}
	if c.NumWorkers <= 0 {
		return errors.New("num workers must be positive")
	}
	if c.PollInterval <= 0 {
		return errors.New("poll interval must be positive")
	}
	if c.GracePeriod <= 0 {
		return errors.New("grace period must be positive")
	}
	return nil
}

type Pool struct {
	repo         domain.JobRepository
	processJob   *application.ProcessJob
	handler      application.JobHandler
	numWorkers   int
	pollInterval time.Duration
	gracePeriod  time.Duration
	wg           sync.WaitGroup
	cancel       context.CancelFunc
}

func NewPool(cfg PoolConfig) (*Pool, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("worker pool: invalid config: %w", err)
	}
	return &Pool{
		repo:         cfg.Repo,
		processJob:   cfg.ProcessJob,
		handler:      cfg.Handler,
		numWorkers:   cfg.NumWorkers,
		pollInterval: cfg.PollInterval,
		gracePeriod:  cfg.GracePeriod,
	}, nil
}

func (p *Pool) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)
	p.wg.Add(p.numWorkers)
	for i := 0; i < p.numWorkers; i++ {
		go p.runWorker(ctx, i)
	}
}

func (p *Pool) Shutdown(ctx context.Context) error {
	if p.cancel == nil {
		return errors.New("worker pool: shutdown called before start")
	}

	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(p.gracePeriod):
		return ErrGracePeriodExpired
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()

	for {
		if ctx.Err() != nil {
			return
		}

		job, err := p.repo.Dequeue(ctx)
		if err != nil {
			if errors.Is(err, domain.ErrNoJobsAvailable) {
				select {
				case <-time.After(p.pollInterval):
					continue
				case <-ctx.Done():
					return
				}
			}
			slog.Error("worker: dequeue failed", "worker_id", id, "error", err)
			select {
			case <-time.After(p.pollInterval):
				continue
			case <-ctx.Done():
				return
			}
		}

		if err := p.processJob.Execute(context.Background(), job, p.handler); err != nil {
			slog.Warn("worker: job failed", "worker_id", id, "job_id", job.ID, "error", err)
		} else {
			slog.Info("worker: job completed", "worker_id", id, "job_id", job.ID)
		}
	}
}
