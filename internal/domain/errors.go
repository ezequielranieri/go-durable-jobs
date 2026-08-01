package domain

import "errors"

var (
	ErrJobNotFound             = errors.New("job not found")
	ErrJobNotDead              = errors.New("job is not in dead status")
	ErrDuplicateIdempotencyKey = errors.New("duplicate idempotency key")
	ErrNoJobsAvailable         = errors.New("no jobs available to dequeue")
	ErrInvalidStatusTransition = errors.New("invalid status transition")
)
