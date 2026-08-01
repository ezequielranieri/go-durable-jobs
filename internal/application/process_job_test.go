package application_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ezequielranieri/go-durable-jobs/internal/application"
	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

const testBaseDelay = 100 * time.Millisecond

func TestProcessJob_HandleOK(t *testing.T) {
	repo := newFakeJobRepository()
	handler := &fakeJobHandler{result: nil}
	uc := application.NewProcessJob(repo, testBaseDelay, nil)

	ctx := context.Background()
	job := makeRunningJob(repo, 0)

	err := uc.Execute(ctx, job, handler)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	repo.mu.Lock()
	saved := repo.jobs[job.ID]
	repo.mu.Unlock()

	if saved.Status != domain.StatusCompleted {
		t.Errorf("expected status completed, got %v", saved.Status)
	}
	if saved.CompletedAt == nil || saved.CompletedAt.IsZero() {
		t.Error("expected completed_at to be set")
	}
}

func TestProcessJob_HandleError(t *testing.T) {
	repo := newFakeJobRepository()
	handler := &fakeJobHandler{result: errors.New("handler boom")}
	uc := application.NewProcessJob(repo, testBaseDelay, nil)

	ctx := context.Background()
	job := makeRunningJob(repo, 0)

	err := uc.Execute(ctx, job, handler)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "job failed") {
		t.Errorf("expected 'job failed' in error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "handler boom") {
		t.Errorf("expected 'handler boom' in error, got %q", err.Error())
	}

	repo.mu.Lock()
	saved := repo.jobs[job.ID]
	failedError := repo.lastFailedError
	failedNextAvailable := repo.lastFailedNextAvailable
	repo.mu.Unlock()

	if saved.Status != domain.StatusPending {
		t.Errorf("expected status pending, got %v", saved.Status)
	}
	if saved.Attempts != 1 {
		t.Errorf("expected attempts=1, got %d", saved.Attempts)
	}
	if saved.LastError == nil || *saved.LastError != "handler boom" {
		t.Errorf("expected last_error='handler boom', got %v", saved.LastError)
	}
	if failedError != "handler boom" {
		t.Errorf("expected MarkFailed called with 'handler boom', got %q", failedError)
	}

	now := time.Now()
	expectedMin := now.Add(testBaseDelay)
	expectedMax := now.Add(testBaseDelay + testBaseDelay/2 + time.Second)
	if failedNextAvailable.Before(expectedMin) || failedNextAvailable.After(expectedMax) {
		t.Errorf("nextAvailableAt %v outside expected range [%v, %v]",
			failedNextAvailable, expectedMin, expectedMax)
	}
}

func TestProcessJob_HandlePanic(t *testing.T) {
	repo := newFakeJobRepository()
	handler := &fakeJobHandler{panicMsg: "panic attack"}
	uc := application.NewProcessJob(repo, testBaseDelay, nil)

	ctx := context.Background()
	job := makeRunningJob(repo, 0)

	err := uc.Execute(ctx, job, handler)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("expected 'panic' in error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "panic attack") {
		t.Errorf("expected 'panic attack' in error, got %q", err.Error())
	}

	repo.mu.Lock()
	saved := repo.jobs[job.ID]
	failedError := repo.lastFailedError
	repo.mu.Unlock()

	if saved.Status != domain.StatusPending {
		t.Errorf("expected status pending, got %v", saved.Status)
	}
	if saved.Attempts != 1 {
		t.Errorf("expected attempts=1, got %d", saved.Attempts)
	}
	if !strings.Contains(failedError, "panic") {
		t.Errorf("expected MarkFailed called with 'panic' in message, got %q", failedError)
	}
}

func TestProcessJob_MarkCompletedFails(t *testing.T) {
	repo := newFakeJobRepository()
	repo.markCompletedErr = errors.New("db error")
	handler := &fakeJobHandler{result: nil}
	uc := application.NewProcessJob(repo, testBaseDelay, nil)

	ctx := context.Background()
	job := makeRunningJob(repo, 0)

	err := uc.Execute(ctx, job, handler)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "mark completed") {
		t.Errorf("expected 'mark completed' in error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "db error") {
		t.Errorf("expected 'db error' in error, got %q", err.Error())
	}
}

func TestProcessJob_HandleAndMarkFailedBothFail(t *testing.T) {
	repo := newFakeJobRepository()
	repo.markFailedErr = errors.New("disk full")
	handler := &fakeJobHandler{result: errors.New("handler boom")}
	uc := application.NewProcessJob(repo, testBaseDelay, nil)

	ctx := context.Background()
	job := makeRunningJob(repo, 0)

	err := uc.Execute(ctx, job, handler)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "handler boom") {
		t.Errorf("expected 'handler boom' in error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "disk full") {
		t.Errorf("expected 'disk full' in error, got %q", err.Error())
	}
}

func TestProcessJob_PanicAndMarkFailedBothFail(t *testing.T) {
	repo := newFakeJobRepository()
	repo.markFailedErr = errors.New("disk full")
	handler := &fakeJobHandler{panicMsg: "panic attack"}
	uc := application.NewProcessJob(repo, testBaseDelay, nil)

	ctx := context.Background()
	job := makeRunningJob(repo, 0)

	err := uc.Execute(ctx, job, handler)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("expected 'panic' in error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "disk full") {
		t.Errorf("expected 'disk full' in error, got %q", err.Error())
	}

	repo.mu.Lock()
	saved := repo.jobs[job.ID]
	repo.mu.Unlock()

	if saved.Status != domain.StatusRunning {
		t.Errorf("expected status running (MarkFailed was not applied), got %v", saved.Status)
	}
	if saved.Attempts != 0 {
		t.Errorf("expected attempts=0 (MarkFailed was not applied), got %d", saved.Attempts)
	}
}
