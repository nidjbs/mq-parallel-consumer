package engine

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Handler processes a single message. It runs on worker goroutines.
type Handler func(ctx context.Context, msg *Message) error

// RetryPolicy controls in-lane retries. Zero value means no retry.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// Config is fully configurable; every field has zero-value semantics.
type Config struct {
	Mode             Mode
	Lanes            int           // KeyOrdered: lanes per partition
	Concurrency      int           // Unordered: per-partition concurrency; the hard in-flight cap in this mode
	MaxInFlight      int           // per-partition in-flight soft target that triggers pause; 0 = derived. Hard memory bounds are the bounded lane queues (Lanes x QueueSize) in KeyOrdered and the Concurrency semaphore in Unordered.
	QueueSize        int           // per-lane bounded queue depth
	CommitInterval   time.Duration // 0 = commit immediately when the contiguous base advances
	PollTimeout      time.Duration
	RebalanceTimeout time.Duration
	Retry            RetryPolicy
	OnDiscard        func(ctx context.Context, msg *Message, err error)
	KeyExtractor     func(*Message) string // nil = use msg.Key
}

// DefaultConfig returns the recommended configuration.
func DefaultConfig() Config {
	return Config{
		Mode:             KeyOrdered,
		Lanes:            8,
		Concurrency:      8,
		QueueSize:        16,
		CommitInterval:   100 * time.Millisecond,
		PollTimeout:      100 * time.Millisecond,
		RebalanceTimeout: 3 * time.Second,
	}
}

// withDefaults applies defaults to zero fields and validates. CommitInterval
// and Retry.MaxAttempts keep 0 as a meaningful value.
func (c Config) withDefaults() (Config, error) {
	if c.Mode != KeyOrdered && c.Mode != Unordered {
		return c, fmt.Errorf("%w: unknown mode %d", ErrInvalidConfig, c.Mode)
	}
	d := DefaultConfig()
	if c.Lanes == 0 {
		c.Lanes = d.Lanes
	}
	if c.Concurrency == 0 {
		c.Concurrency = d.Concurrency
	}
	if c.QueueSize == 0 {
		c.QueueSize = d.QueueSize
	}
	// CommitInterval is NOT defaulted: 0 is a meaningful value (commit-on-advance).
	if c.PollTimeout == 0 {
		c.PollTimeout = d.PollTimeout
	}
	if c.RebalanceTimeout == 0 {
		c.RebalanceTimeout = d.RebalanceTimeout
	}
	concurrency := c.Concurrency
	if c.Mode == KeyOrdered {
		concurrency = c.Lanes
	}
	if c.MaxInFlight == 0 {
		c.MaxInFlight = concurrency * c.QueueSize
	}
	if c.Lanes < 0 || c.Concurrency < 0 || c.QueueSize <= 0 || c.MaxInFlight <= 0 ||
		c.CommitInterval < 0 || c.PollTimeout <= 0 || c.RebalanceTimeout <= 0 {
		return c, fmt.Errorf("%w: non-positive value in %+v", ErrInvalidConfig, c)
	}
	if c.Retry.MaxAttempts < 0 {
		return c, errors.New("swimlane: Retry.MaxAttempts must be >= 0")
	}
	return c, nil
}
