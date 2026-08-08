package domain

import (
	"testing"
	"time"
)

func TestNextBackoff(t *testing.T) {
	tests := []struct {
		name      string
		attempt   int
		baseDelay time.Duration
		min       time.Duration
		max       time.Duration
	}{
		{
			name:      "attempt 1 returns base delay + jitter",
			attempt:   1,
			baseDelay: time.Second,
			min:       time.Second,
			max:       time.Second + time.Second/2,
		},
		{
			name:      "attempt 2 returns 2x base + jitter",
			attempt:   2,
			baseDelay: time.Second,
			min:       2 * time.Second,
			max:       3 * time.Second,
		},
		{
			name:      "attempt 3 returns 4x base + jitter",
			attempt:   3,
			baseDelay: time.Second,
			min:       4 * time.Second,
			max:       6 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NextBackoff(tt.attempt, tt.baseDelay)
			if got < tt.min {
				t.Errorf("NextBackoff(%d, %v) = %v, want >= %v", tt.attempt, tt.baseDelay, got, tt.min)
			}
			if got > tt.max {
				t.Errorf("NextBackoff(%d, %v) = %v, want <= %v", tt.attempt, tt.baseDelay, got, tt.max)
			}
		})
	}
}

func TestNextBackoff_NonPositiveAttempt(t *testing.T) {
	t.Run("attempt 0 returns base delay", func(t *testing.T) {
		got := NextBackoff(0, time.Second)
		if got < time.Second || got > time.Second+time.Second/2 {
			t.Errorf("NextBackoff(0, 1s) = %v, want ~1s", got)
		}
	})
	t.Run("negative attempt returns base delay", func(t *testing.T) {
		got := NextBackoff(-1, time.Second)
		if got < time.Second || got > time.Second+time.Second/2 {
			t.Errorf("NextBackoff(-1, 1s) = %v, want ~1s", got)
		}
	})
}

func TestNextBackoff_JitterVariability(t *testing.T) {
	const iterations = 100
	results := make(map[time.Duration]int)
	for i := 0; i < iterations; i++ {
		d := NextBackoff(3, time.Second)
		results[d]++
	}
	if len(results) < 2 {
		t.Errorf("jitter produced no variability across %d calls: got %d distinct values", iterations, len(results))
	}
}

func TestNextBackoff_JitterNeverNegative(t *testing.T) {
	for i := 0; i < 1000; i++ {
		d := NextBackoff(3, time.Second)
		if d <= 0 {
			t.Fatalf("NextBackoff(3, 1s) = %v, want > 0", d)
		}
	}
}



