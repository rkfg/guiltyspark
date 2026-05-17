package retry

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoff_GetDelay_table(t *testing.T) {
	cfg := BackoffConfig{
		InitialDelay: 1 * time.Second,
		MaxDelay:     5 * time.Minute,
		Multiplier:   2.0,
	}
	bm := NewBackoffManager(cfg)

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{10, 5 * time.Minute}, // capped
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt%d", tt.attempt), func(t *testing.T) {
			got := bm.GetDelay(tt.attempt)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBackoff_GetDelay_capped_table(t *testing.T) {
	cfg := BackoffConfig{
		InitialDelay: 1 * time.Second,
		MaxDelay:     3 * time.Second,
		Multiplier:   3.0,
	}
	bm := NewBackoffManager(cfg)

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 3 * time.Second}, // capped
		{2, 3 * time.Second}, // capped
		{3, 3 * time.Second}, // capped
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt%d", tt.attempt), func(t *testing.T) {
			got := bm.GetDelay(tt.attempt)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBackoff_ShouldRetry_table(t *testing.T) {
	tests := []struct {
		name     string
		maxRetries int
		attempt  int
		want     bool
	}{
		{"maxRetries=3 attempt=0", 3, 0, true},
		{"maxRetries=3 attempt=1", 3, 1, true},
		{"maxRetries=3 attempt=2", 3, 2, true},
		{"maxRetries=3 attempt=3", 3, 3, false},
		{"maxRetries=0 attempt=0", 0, 0, false},
		{"maxRetries=-1 unlimited attempt=0", -1, 0, true},
		{"maxRetries=-1 unlimited attempt=100", -1, 100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := BackoffConfig{
				InitialDelay: 1 * time.Second,
				MaxDelay:     10 * time.Second,
				Multiplier:   2.0,
				MaxRetries:   tt.maxRetries,
			}
			bm := NewBackoffManager(cfg)
			assert.Equal(t, tt.want, bm.ShouldRetry(tt.attempt))
		})
	}
}
