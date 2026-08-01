package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ezequielranieri/go-durable-jobs/internal/application"
	"github.com/ezequielranieri/go-durable-jobs/internal/domain"
)

func TestEnqueueJob_NewIdempotencyKey(t *testing.T) {
	repo := newFakeJobRepository()
	uc := application.NewEnqueueJob(repo, nil)
	ctx := context.Background()

	result, err := uc.Execute(ctx, application.EnqueueJobRequest{
		Type:           "send_email",
		Payload:        json.RawMessage(`{"to":"user@example.com"}`),
		IdempotencyKey: "key-001",
		Priority:       0,
		Delay:          0,
		MaxAttempts:    0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.AlreadyExisted {
		t.Error("expected AlreadyExisted=false for new key")
	}
	if result.Job == nil {
		t.Fatal("expected non-nil job")
	}
	if result.Job.Status != domain.StatusPending {
		t.Errorf("expected status pending, got %v", result.Job.Status)
	}
	if result.Job.MaxAttempts != 5 {
		t.Errorf("expected default max_attempts=5, got %d", result.Job.MaxAttempts)
	}
	if result.Job.Type != "send_email" {
		t.Errorf("expected type 'send_email', got %q", result.Job.Type)
	}
}

func TestEnqueueJob_DuplicateIdempotencyKey(t *testing.T) {
	repo := newFakeJobRepository()
	uc := application.NewEnqueueJob(repo, nil)
	ctx := context.Background()

	req := application.EnqueueJobRequest{
		Type:           "send_email",
		Payload:        json.RawMessage(`{"to":"user@example.com"}`),
		IdempotencyKey: "key-001",
		Priority:       0,
		Delay:          0,
		MaxAttempts:    0,
	}

	first, err := uc.Execute(ctx, req)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	if first.AlreadyExisted {
		t.Error("expected AlreadyExisted=false on first create")
	}

	second, err := uc.Execute(ctx, req)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if !second.AlreadyExisted {
		t.Error("expected AlreadyExisted=true on duplicate")
	}
	if second.Job.ID != first.Job.ID {
		t.Errorf("expected same job ID %v, got %v", first.Job.ID, second.Job.ID)
	}
}

func TestEnqueueJob_EmptyType(t *testing.T) {
	repo := newFakeJobRepository()
	uc := application.NewEnqueueJob(repo, nil)
	ctx := context.Background()

	_, err := uc.Execute(ctx, application.EnqueueJobRequest{
		Type:           "",
		Payload:        json.RawMessage(`{}`),
		IdempotencyKey: "key-002",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, application.ErrInvalidRequest) {
		t.Errorf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestEnqueueJob_EmptyIdempotencyKey(t *testing.T) {
	repo := newFakeJobRepository()
	uc := application.NewEnqueueJob(repo, nil)
	ctx := context.Background()

	_, err := uc.Execute(ctx, application.EnqueueJobRequest{
		Type:           "test",
		Payload:        json.RawMessage(`{}`),
		IdempotencyKey: "",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, application.ErrInvalidRequest) {
		t.Errorf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestEnqueueJob_NilPayload(t *testing.T) {
	repo := newFakeJobRepository()
	uc := application.NewEnqueueJob(repo, nil)
	ctx := context.Background()

	_, err := uc.Execute(ctx, application.EnqueueJobRequest{
		Type:           "test",
		Payload:        nil,
		IdempotencyKey: "key-003",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, application.ErrInvalidRequest) {
		t.Errorf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestEnqueueJob_CustomMaxAttempts(t *testing.T) {
	repo := newFakeJobRepository()
	uc := application.NewEnqueueJob(repo, nil)
	ctx := context.Background()

	result, err := uc.Execute(ctx, application.EnqueueJobRequest{
		Type:           "test",
		Payload:        json.RawMessage(`{}`),
		IdempotencyKey: "key-004",
		MaxAttempts:    3,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Job.MaxAttempts != 3 {
		t.Errorf("expected max_attempts=3, got %d", result.Job.MaxAttempts)
	}
}

func TestEnqueueJob_NegativeMaxAttempts(t *testing.T) {
	repo := newFakeJobRepository()
	uc := application.NewEnqueueJob(repo, nil)
	ctx := context.Background()

	_, err := uc.Execute(ctx, application.EnqueueJobRequest{
		Type:           "test",
		Payload:        json.RawMessage(`{}`),
		IdempotencyKey: "key-006",
		MaxAttempts:    -3,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, application.ErrInvalidRequest) {
		t.Errorf("expected ErrInvalidRequest, got %v", err)
	}
	if repo.Created() != 0 {
		t.Errorf("expected no job created, got %d", repo.Created())
	}
}

func TestEnqueueJob_MaxAttemptsOne(t *testing.T) {
	repo := newFakeJobRepository()
	uc := application.NewEnqueueJob(repo, nil)
	ctx := context.Background()

	result, err := uc.Execute(ctx, application.EnqueueJobRequest{
		Type:           "test",
		Payload:        json.RawMessage(`{}`),
		IdempotencyKey: "key-007",
		MaxAttempts:    1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Job.MaxAttempts != 1 {
		t.Errorf("expected max_attempts=1, got %d", result.Job.MaxAttempts)
	}
}

func TestEnqueueJob_WithDelay(t *testing.T) {
	repo := newFakeJobRepository()
	uc := application.NewEnqueueJob(repo, nil)
	ctx := context.Background()

	now := time.Now()

	result, err := uc.Execute(ctx, application.EnqueueJobRequest{
		Type:           "test",
		Payload:        json.RawMessage(`{}`),
		IdempotencyKey: "key-005",
		Delay:          5 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Job.AvailableAt.After(now) {
		t.Error("expected AvailableAt in the future due to delay")
	}
}
