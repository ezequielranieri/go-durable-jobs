package handler_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ezequielranieri/go-durable-jobs/internal/application"
	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
	"github.com/ezequielranieri/go-durable-jobs/internal/handler"
)

func newTestServer(repo *fakeRepo) http.Handler {
	metricsHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write([]byte("mock metrics exposition\n"))
	})
	s := handler.New(application.NewEnqueueJob(repo, nil), repo, metricsHandler)
	return s.Mux()
}

func doRequest(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.NewDecoder(rec.Body).Decode(out); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
}

func TestPostJobs_Created(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	body := `{"type":"send_email","payload":{"to":"a@b.c"},"idempotency_key":"k1"}`
	rec := doRequest(t, h, http.MethodPost, "/jobs", body)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body=%s)", rec.Code, rec.Body.String())
	}

	var job domain.Job
	decodeBody(t, rec, &job)
	if job.Type != "send_email" {
		t.Errorf("expected type send_email, got %q", job.Type)
	}
	if job.Status != domain.StatusPending {
		t.Errorf("expected status pending, got %q", job.Status)
	}
	if job.MaxAttempts != 5 {
		t.Errorf("expected default max_attempts=5, got %d", job.MaxAttempts)
	}
}

func TestPostJobs_Idempotent(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	body := `{"type":"send_email","payload":{"to":"a@b.c"},"idempotency_key":"same-key"}`
	first := doRequest(t, h, http.MethodPost, "/jobs", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("expected 201 on first, got %d", first.Code)
	}
	var firstJob domain.Job
	decodeBody(t, first, &firstJob)

	second := doRequest(t, h, http.MethodPost, "/jobs", body)
	if second.Code != http.StatusOK {
		t.Fatalf("expected 200 on duplicate, got %d (body=%s)", second.Code, second.Body.String())
	}
	var secondJob domain.Job
	decodeBody(t, second, &secondJob)
	if secondJob.ID != firstJob.ID {
		t.Errorf("expected same job id %v, got %v", firstJob.ID, secondJob.ID)
	}
}

func TestPostJobs_WithOptions(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	body := `{"type":"send_email","payload":{},"idempotency_key":"k-opt","priority":3,"delay_seconds":10,"max_attempts":7}`
	rec := doRequest(t, h, http.MethodPost, "/jobs", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body=%s)", rec.Code, rec.Body.String())
	}

	var job domain.Job
	decodeBody(t, rec, &job)
	if job.Priority != 3 {
		t.Errorf("expected priority 3, got %d", job.Priority)
	}
	if job.MaxAttempts != 7 {
		t.Errorf("expected max_attempts 7, got %d", job.MaxAttempts)
	}
	if !job.AvailableAt.After(job.CreatedAt.Add(9 * time.Second)) {
		t.Error("expected delay_seconds to push available_at into the future")
	}
}

func TestPostJobs_NegativeMaxAttempts(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	body := `{"type":"send_email","payload":{},"idempotency_key":"k-neg","max_attempts":-3}`
	rec := doRequest(t, h, http.MethodPost, "/jobs", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	if repo.createCalls() != 0 {
		t.Error("expected no job persisted when validation rejects the request")
	}
}

func TestPostJobs_EmptyType(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	body := `{"type":"","payload":{},"idempotency_key":"k-empt"}`
	rec := doRequest(t, h, http.MethodPost, "/jobs", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (body=%s)", rec.Code, rec.Body.String())
	}
}

func TestPostJobs_InvalidJSON(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodPost, "/jobs", `{"type": `)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (body=%s)", rec.Code, rec.Body.String())
	}
}

func TestPostJobs_RepoError(t *testing.T) {
	repo := newFakeRepo()
	repo.createErr = errors.New("boom: connection refused")
	h := newTestServer(repo)

	body := `{"type":"send_email","payload":{},"idempotency_key":"k-err"}`
	rec := doRequest(t, h, http.MethodPost, "/jobs", body)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "connection refused") {
		t.Errorf("expected generic error body, got raw error: %s", rec.Body.String())
	}
}

func TestGetJob_OK(t *testing.T) {
	repo := newFakeRepo()
	job := newJob()
	seedJob(repo, job)
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodGet, "/jobs/"+job.ID.String(), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	var got domain.Job
	decodeBody(t, rec, &got)
	if got.ID != job.ID {
		t.Errorf("expected id %v, got %v", job.ID, got.ID)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodGet, "/jobs/"+uuid.New().String(), "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestGetJob_InvalidID(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodGet, "/jobs/not-a-uuid", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestRequeue_OK(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodPost, "/jobs/"+uuid.New().String()+"/requeue", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestRequeue_NotDead(t *testing.T) {
	repo := newFakeRepo()
	repo.requeue = domain.ErrJobNotDead
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodPost, "/jobs/"+uuid.New().String()+"/requeue", "")
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestRequeue_NotFound(t *testing.T) {
	repo := newFakeRepo()
	repo.requeue = domain.ErrJobNotFound
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodPost, "/jobs/"+uuid.New().String()+"/requeue", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestRequeue_InvalidID(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodPost, "/jobs/not-a-uuid/requeue", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestMetrics_RoutesToInjectedHandler(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodGet, "/metrics", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "mock metrics exposition") {
		t.Errorf("expected injected metrics handler output, got %q", rec.Body.String())
	}
}

func TestMethodNotAllowed(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodDelete, "/jobs/"+uuid.New().String(), "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestErrorResponseShape(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodPost, "/jobs", `{"type":"","payload":{},"idempotency_key":"k-shape"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	var payload map[string]string
	decodeBody(t, rec, &payload)
	if payload["error"] == "" {
		t.Errorf("expected {\"error\": ...} body, got %s", rec.Body.String())
	}
}

func TestRequeueErrorResponseShape(t *testing.T) {
	repo := newFakeRepo()
	repo.requeue = domain.ErrJobNotDead
	h := newTestServer(repo)

	rec := doRequest(t, h, http.MethodPost, "/jobs/"+uuid.New().String()+"/requeue", "")
	var payload map[string]string
	decodeBody(t, rec, &payload)
	if payload["error"] == "" {
		t.Errorf("expected {\"error\": ...} body, got %s", rec.Body.String())
	}
}

func TestPayloadPreserved(t *testing.T) {
	repo := newFakeRepo()
	h := newTestServer(repo)

	body := `{"type":"echo","payload":{"msg":"hello world","n":42},"idempotency_key":"k-payload"}`
	rec := doRequest(t, h, http.MethodPost, "/jobs", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	var job domain.Job
	decodeBody(t, rec, &job)
	var payload struct {
		Msg string `json:"msg"`
		N   int    `json:"n"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Msg != "hello world" || payload.N != 42 {
		t.Errorf("unexpected payload: %+v", payload)
	}
}
