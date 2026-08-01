package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/ezequielranieri/go-durable-jobs/internal/application"
	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

type Server struct {
	enqueue        *application.EnqueueJob
	repo           domain.JobRepository
	metricsHandler http.Handler
}

func New(enqueue *application.EnqueueJob, repo domain.JobRepository, metricsHandler http.Handler) *Server {
	return &Server{enqueue: enqueue, repo: repo, metricsHandler: metricsHandler}
}

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.handleEnqueue)
	mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	mux.HandleFunc("POST /jobs/{id}/requeue", s.handleRequeue)
	mux.Handle("GET /metrics", s.metricsHandler)
	return mux
}

const maxBodyBytes = 1 << 20

type enqueueRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
	Priority       domain.Priority `json:"priority"`
	DelaySeconds   int             `json:"delay_seconds"`
	MaxAttempts    int             `json:"max_attempts"`
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer r.Body.Close()

	var body enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	result, err := s.enqueue.Execute(r.Context(), application.EnqueueJobRequest{
		Type:           body.Type,
		Payload:        body.Payload,
		IdempotencyKey: body.IdempotencyKey,
		Priority:       body.Priority,
		Delay:          time.Duration(body.DelaySeconds) * time.Second,
		MaxAttempts:    body.MaxAttempts,
	})
	if err != nil {
		if errors.Is(err, application.ErrInvalidRequest) {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		slog.Error("enqueue", "err", err)
		writeError(w, http.StatusInternalServerError, errors.New("internal server error"))
		return
	}

	status := http.StatusCreated
	if result.AlreadyExisted {
		status = http.StatusOK
	}
	writeJSON(w, status, result.Job)
}
