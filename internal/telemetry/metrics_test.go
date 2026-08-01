package telemetry_test

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ezequielranieri/go-durable-jobs/internal/application"
	"github.com/ezequielranieri/go-durable-jobs/internal/telemetry"
)

func TestMetrics_Exposition(t *testing.T) {
	m := telemetry.NewMetrics()

	m.IncJobsEnqueued()
	m.IncJobsEnqueued()
	m.IncJobsProcessed(application.JobCompleted)
	m.IncJobsProcessed(application.JobFailed)
	m.IncJobsInFlight()
	m.DecJobsInFlight()
	m.ObserveJobProcessingDuration(1500 * time.Millisecond)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("expected prometheus text content-type, got %q", ct)
	}

	body := rec.Body.String()
	expected := []string{
		"jobs_enqueued_total 2",
		`jobs_processed_total{result="completed"} 1`,
		`jobs_processed_total{result="failed"} 1`,
		"jobs_in_flight 0",
		"job_processing_duration_seconds_count 1",
		"job_processing_duration_seconds_sum 1.5",
	}
	for _, want := range expected {
		if !strings.Contains(body, want) {
			t.Errorf("expected %q in exposition, got:\n%s", want, body)
		}
	}
}
