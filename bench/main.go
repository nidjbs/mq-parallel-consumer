// bench measures the concurrency gain of the swimlane consumer versus a
// sequential consumer, under an IO-bound handler.
//
// Run: go run ./bench
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"mq-parallel-consumer"
)

// benchBackend is a minimal in-memory backend for measuring the engine only
// (no network/fetch cost).
type benchBackend struct {
	mu       sync.Mutex
	messages []swimlane.Message
	h        swimlane.RebalanceHandler
}

func (b *benchBackend) SetRebalanceHandler(h swimlane.RebalanceHandler) {
	b.h = h
	b.mu.Lock()
	m := make(map[swimlane.TopicPartition]swimlane.Offset)
	for _, msg := range b.messages {
		m[msg.TP()] = 0
	}
	b.mu.Unlock()
	if len(m) > 0 {
		h.OnAssigned(context.Background(), m)
	}
}

func (b *benchBackend) Subscribe(topics []string) error { return nil }

func (b *benchBackend) Poll(ctx context.Context, maxWait time.Duration) ([]swimlane.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.messages) == 0 {
		return nil, nil
	}
	batch := b.messages
	b.messages = nil
	return batch, nil
}

func (b *benchBackend) Commit(ctx context.Context, commits map[swimlane.TopicPartition]swimlane.Offset) error {
	return nil
}

func (b *benchBackend) Pause(parts []swimlane.TopicPartition) error  { return nil }
func (b *benchBackend) Resume(parts []swimlane.TopicPartition) error { return nil }
func (b *benchBackend) Close(ctx context.Context) error              { return nil }

func newBenchBackend(n int) *benchBackend {
	msgs := make([]swimlane.Message, 0, n)
	for i := 0; i < n; i++ {
		msgs = append(msgs, swimlane.Message{
			Topic: "t", Partition: 0, Offset: swimlane.Offset(i),
			Key: []byte(fmt.Sprintf("key-%d", i)), // distinct keys -> even lane spread
		})
	}
	return &benchBackend{messages: msgs}
}

// runSwimlane processes n messages through the engine and returns elapsed.
func runSwimlane(n, lanes int, handler swimlane.Handler) time.Duration {
	be := newBenchBackend(n)
	cfg := swimlane.DefaultConfig()
	cfg.Lanes = lanes
	cfg.QueueSize = 1024
	cfg.CommitInterval = time.Millisecond
	c, err := swimlane.New(be, cfg)
	if err != nil {
		panic(err)
	}
	if err := c.Subscribe([]string{"t"}, handler); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	start := time.Now()
	tp := swimlane.TopicPartition{Topic: "t", Partition: 0}
	for {
		s := c.Stats()
		ps := s.PerPartition[tp]
		if ps.BaseOffset == swimlane.Offset(n) && ps.InFlight == 0 {
			break
		}
		if time.Since(start) > 30*time.Second {
			panic("timed out waiting for completion")
		}
		time.Sleep(time.Millisecond)
	}
	elapsed := time.Since(start)
	c.Stop()
	return elapsed
}

func main() {
	const n = 2000
	handler := func(ctx context.Context, m *swimlane.Message) error {
		time.Sleep(time.Millisecond) // simulate IO wait (DB/RPC/HTTP)
		return nil
	}

	// sequential baseline
	start := time.Now()
	for i := 0; i < n; i++ {
		handler(context.Background(), nil)
	}
	seq := time.Since(start)

	fmt.Printf("handler: %s / msg (IO-bound)\n", time.Millisecond)
	fmt.Printf("%-16s %12s %12s %8s\n", "config", "elapsed", "msg/s", "speedup")
	printRow := func(name string, d time.Duration) {
		fmt.Printf("%-16s %12s %12.0f %7.1fx\n",
			name, d.Round(time.Millisecond), float64(n)/d.Seconds(), seq.Seconds()/d.Seconds())
	}
	printRow("sequential", seq)
	for _, lanes := range []int{1, 4, 8, 16, 32} {
		printRow(fmt.Sprintf("swimlane L=%d", lanes), runSwimlane(n, lanes, handler))
	}
}
