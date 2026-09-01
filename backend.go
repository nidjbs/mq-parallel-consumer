package swimlane

import (
	"context"
	"time"
)

// Mode is the ordering guarantee offered by the engine.
type Mode int

const (
	KeyOrdered Mode = iota // same key serial, different keys concurrent
	Unordered              // fully concurrent, no ordering
)

// RebalanceHandler is implemented by the core; adapters call it on rebalance.
type RebalanceHandler interface {
	// OnRevoked is called synchronously on the poll goroutine; the core
	// drains in-flight messages and commits final offsets before returning.
	OnRevoked(ctx context.Context, revoked []TopicPartition) error
	// OnAssigned is called when partitions are assigned. offset is the
	// partition's start position (may be 0 if unknown).
	OnAssigned(ctx context.Context, assigned map[TopicPartition]Offset) error
}

// Backend is the SPI adapters implement (transport layer).
type Backend interface {
	SetRebalanceHandler(h RebalanceHandler)
	// Subscribe registers topics to consume.
	Subscribe(topics []string) error
	// Poll blocks up to maxWait and returns a batch of messages.
	Poll(ctx context.Context, maxWait time.Duration) ([]Message, error)
	// Commit persists offsets; the value is the "next offset to consume".
	Commit(ctx context.Context, commits map[TopicPartition]Offset) error
	Pause(parts []TopicPartition) error
	Resume(parts []TopicPartition) error
	Close(ctx context.Context) error
}
