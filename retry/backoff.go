package retry

import (
	"math"
	"time"
)

type BackoffConfig struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	MaxRetries   int // -1 = unlimited
}

type BackoffManager struct {
	Config BackoffConfig
}

func NewBackoffManager(cfg BackoffConfig) *BackoffManager {
	return &BackoffManager{Config: cfg}
}

func (b *BackoffManager) GetDelay(attempt int) time.Duration {
	delay := min(time.Duration(float64(b.Config.InitialDelay) * math.Pow(b.Config.Multiplier, float64(attempt))), b.Config.MaxDelay)
	return delay
}

func (b *BackoffManager) ShouldRetry(attempt int) bool {
	return b.Config.MaxRetries < 0 || attempt < b.Config.MaxRetries
}
