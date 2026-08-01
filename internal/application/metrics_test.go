package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ezequielranieri/go-durable-jobs/internal/application"
)

type recordingMetrics struct {
	mu          sync.Mutex
	enqueued    int
	completed   int
	failed      int
	inFlight    int
	maxInFlight int
	durationN   int
}

func (r *recordingMetrics) IncJobsEnqueued() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enqueued++
}

func (r *recordingMetrics) IncJobsProcessed(result application.JobResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch result {
	case application.JobCompleted:
		r.completed++
	case application.JobFailed:
		r.failed++
	}
}

func (r *recordingMetrics) IncJobsInFlight() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight++
	if r.inFlight > r.maxInFlight {
		r.maxInFlight = r.inFlight
	}
}

func (r *recordingMetrics) DecJobsInFlight() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight--
}

func (r *recordingMetrics) ObserveJobProcessingDuration(time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.durationN++
}

func (r *recordingMetrics) snapshots() (enqueued, completed, failed, inFlight, maxInFlight, durationN int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enqueued, r.completed, r.failed, r.inFlight, r.maxInFlight, r.durationN
}

func TestEnqueueJob_MetricsIncOnlyOnNew(t *testing.T) {
	repo := newFakeJobRepository()
	m := &recordingMetrics{}
	uc := application.NewEnqueueJob(repo, m)
	ctx := context.Background()

	req := application.EnqueueJobRequest{
		Type:           "test",
		Payload:        json.RawMessage(`{}`),
		IdempotencyKey: "key-m1",
	}

	if _, err := uc.Execute(ctx, req); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err := uc.Execute(ctx, req); err != nil {
		t.Fatalf("duplicate create: %v", err)
	}

	enqueued, _, _, _, _, _ := m.snapshots()
	if enqueued != 1 {
		t.Errorf("expected 1 enqueued (only new job), got %d", enqueued)
	}
}

func TestProcessJob_MetricsCompleted(t *testing.T) {
	repo := newFakeJobRepository()
	m := &recordingMetrics{}
	uc := application.NewProcessJob(repo, testBaseDelay, m)
	handler := &fakeJobHandler{result: nil}

	ctx := context.Background()
	job := makeRunningJob(repo, 0)

	if err := uc.Execute(ctx, job, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	enqueued, completed, failed, inFlight, maxInFlight, durationN := m.snapshots()
	if enqueued != 0 {
		t.Errorf("expected 0 enqueued, got %d", enqueued)
	}
	if completed != 1 {
		t.Errorf("expected 1 completed, got %d", completed)
	}
	if failed != 0 {
		t.Errorf("expected 0 failed, got %d", failed)
	}
	if inFlight != 0 {
		t.Errorf("expected in-flight back to 0, got %d", inFlight)
	}
	if maxInFlight != 1 {
		t.Errorf("expected max in-flight 1, got %d", maxInFlight)
	}
	if durationN != 1 {
		t.Errorf("expected 1 duration observation, got %d", durationN)
	}
}

func TestProcessJob_MetricsFailed(t *testing.T) {
	repo := newFakeJobRepository()
	m := &recordingMetrics{}
	uc := application.NewProcessJob(repo, testBaseDelay, m)
	handler := &fakeJobHandler{result: errors.New("handler boom")}

	ctx := context.Background()
	job := makeRunningJob(repo, 0)

	if err := uc.Execute(ctx, job, handler); err == nil {
		t.Fatal("expected error, got nil")
	}

	_, completed, failed, inFlight, _, _ := m.snapshots()
	if completed != 0 {
		t.Errorf("expected 0 completed, got %d", completed)
	}
	if failed != 1 {
		t.Errorf("expected 1 failed, got %d", failed)
	}
	if inFlight != 0 {
		t.Errorf("expected in-flight back to 0, got %d", inFlight)
	}
}

func TestProcessJob_MetricsPanic(t *testing.T) {
	repo := newFakeJobRepository()
	m := &recordingMetrics{}
	uc := application.NewProcessJob(repo, testBaseDelay, m)
	handler := &fakeJobHandler{panicMsg: "panic attack"}

	ctx := context.Background()
	job := makeRunningJob(repo, 0)

	if err := uc.Execute(ctx, job, handler); err == nil {
		t.Fatal("expected error, got nil")
	}

	_, completed, failed, inFlight, _, durationN := m.snapshots()
	if completed != 0 {
		t.Errorf("expected 0 completed, got %d", completed)
	}
	if failed != 1 {
		t.Errorf("expected 1 failed, got %d", failed)
	}
	if inFlight != 0 {
		t.Errorf("expected in-flight back to 0, got %d", inFlight)
	}
	if durationN != 1 {
		t.Errorf("expected 1 duration observation, got %d", durationN)
	}
}

func TestProcessJob_MetricsMarkCompletedFails(t *testing.T) {
	repo := newFakeJobRepository()
	repo.markCompletedErr = errors.New("db error")
	m := &recordingMetrics{}
	uc := application.NewProcessJob(repo, testBaseDelay, m)
	handler := &fakeJobHandler{result: nil}

	ctx := context.Background()
	job := makeRunningJob(repo, 0)

	if err := uc.Execute(ctx, job, handler); err == nil {
		t.Fatal("expected error, got nil")
	}

	// Definición acordada: un Execute que no se resolvió limpio (fallo de
	// persistencia posterior) cuenta como "failed", no como "completed".
	_, completed, failed, inFlight, _, _ := m.snapshots()
	if completed != 0 {
		t.Errorf("expected 0 completed, got %d", completed)
	}
	if failed != 1 {
		t.Errorf("expected 1 failed, got %d", failed)
	}
	if inFlight != 0 {
		t.Errorf("expected in-flight back to 0, got %d", inFlight)
	}
}
