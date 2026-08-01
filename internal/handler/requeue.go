package handler

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

func (s *Server) handleRequeue(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if err := s.repo.Requeue(r.Context(), id); err != nil {
		switch {
		case errors.Is(err, domain.ErrJobNotFound):
			writeError(w, http.StatusNotFound, err)
		case errors.Is(err, domain.ErrJobNotDead):
			writeError(w, http.StatusConflict, err)
		default:
			slog.Error("requeue", "err", err)
			writeError(w, http.StatusInternalServerError, errors.New("internal server error"))
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
