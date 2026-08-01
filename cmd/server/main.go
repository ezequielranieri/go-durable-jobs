package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ezequielranieri/go-durable-jobs/internal/application"
	"github.com/ezequielranieri/go-durable-jobs/internal/config"
	"github.com/ezequielranieri/go-durable-jobs/internal/handler"
	"github.com/ezequielranieri/go-durable-jobs/internal/infrastructure/postgres"
	"github.com/ezequielranieri/go-durable-jobs/internal/infrastructure/worker"
	"github.com/ezequielranieri/go-durable-jobs/internal/telemetry"
)

type echoHandler struct{}

func (h *echoHandler) Handle(_ context.Context, jobType string, payload json.RawMessage) error {
	slog.Info("echo handler processing job", "type", jobType, "payload", string(payload))
	return nil
}

func main() {
	os.Exit(run())
}

func run() int {
	slog.Info("go-durable-jobs starting")

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		return 1
	}

	pgxPool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("database", "err", err)
		return 1
	}
	defer pgxPool.Close()

	repo := postgres.NewPostgresJobRepository(pgxPool)
	tm := telemetry.NewMetrics()
	processJob := application.NewProcessJob(repo, cfg.BaseBackoffDelay, tm)
	echoH := &echoHandler{}

	wp, err := worker.NewPool(worker.PoolConfig{
		Repo:         repo,
		ProcessJob:   processJob,
		Handler:      echoH,
		NumWorkers:   cfg.NumWorkers,
		PollInterval: cfg.PollInterval,
		GracePeriod:  cfg.GracePeriod,
	})
	if err != nil {
		slog.Error("worker pool", "err", err)
		return 1
	}

	wp.Start(context.Background())
	slog.Info("worker pool started", "num_workers", cfg.NumWorkers)

	enqueue := application.NewEnqueueJob(repo, tm)
	handlerSrv := handler.New(enqueue, repo, tm.Handler())
	srv := &http.Server{
		Addr:    ":" + cfg.AppPort,
		Handler: handlerSrv.Mux(),
	}

	fatalCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatalCh <- err
		}
	}()
	slog.Info("http server listening", "addr", srv.Addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("signal received, starting graceful shutdown", "signal", sig)
	case err := <-fatalCh:
		slog.Error("http server", "err", err)
		return 1
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.GracePeriod+5*time.Second)
	defer shutdownCancel()

	// 1. HTTP primero: dejar de aceptar requests nuevos. Un job que se acepte
	//    acá y no haya worker vivo para tomarlo quedaría huérfano.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http server shutdown", "err", err)
	}

	// 2. Workers después: drenar jobs en curso dentro del grace period.
	switch err := wp.Shutdown(shutdownCtx); {
	case err == nil:
		slog.Info("shutdown complete")
		return 0

	case errors.Is(err, worker.ErrGracePeriodExpired):
		slog.Error("shutdown: grace period expired, some workers still running", "err", err)
		slog.Warn("dando margen final de 3s antes de salir para reducir ventana de corte")
		// Go no permite esperar una goroutine forzosamente. El return 2 hace
		// que main() llame a os.Exit(2), que mata todo inmediatamente. Este
		// sleep es un trade-off consciente: reduce la probabilidad de cortar
		// un job a mitad sin poder resolverlo del todo.
		time.Sleep(3 * time.Second)
		return 2

	default:
		slog.Error("shutdown error", "err", err)
		return 1
	}
}
