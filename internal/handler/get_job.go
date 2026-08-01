package handler

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	job, err := s.repo.FindByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			writeError(w, http.StatusNotFound, err)
			return
		}
		slog.Error("get job", "err", err)
		writeError(w, http.StatusInternalServerError, errors.New("internal server error"))
		return
	}

	writeJSON(w, http.StatusOK, job)
}
