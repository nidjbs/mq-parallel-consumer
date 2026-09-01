package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeBackend drives the engine deterministically without a real MQ.
type fakeBackend struct {
	mu      sync.Mutex
	queue   []Message
	commits []map[TopicPartition]Offset
	paused  map[TopicPartition]bool
	h       RebalanceHandler
	closed  bool
}

func (f *fakeBackend) SetRebalanceHandler(h RebalanceHandler) {
	f.mu.Lock()
	f.h = h
	// simulate assignment of the partitions present in the queued messages
	m := make(map[TopicPartition]Offset)
	for _, msg := range f.queue {
		m[msg.TP()] = 0
	}
	f.mu.Unlock()
	if len(m) > 0 {
		h.OnAssigned(context.Background(), m)
	}
}
func (f *fakeBackend) Subscribe(topics []string) error { return nil }
func (f *fakeBackend) Poll(ctx context.Context, maxWait time.Duration) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queue) == 0 {
		return nil, nil
	}
	batch := f.queue
	f.queue = nil
	return batch, nil
}
func (f *fakeBackend) Commit(ctx context.Context, commits map[TopicPartition]Offset) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, commits)
	return nil
}
func (f *fakeBackend) Pause(parts []TopicPartition) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range parts {
		f.paused[p] = true
	}
	return nil
}
func (f *fakeBackend) Resume(parts []TopicPartition) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range parts {
		delete(f.paused, p)
	}
	return nil
}
func (f *fakeBackend) Close(ctx context.Context) error { f.closed = true; return nil }

func newFakeBackend(msgs ...Message) *fakeBackend {
	return &fakeBackend{queue: msgs, paused: map[TopicPartition]bool{}}
}

// committed offsets must form the max contiguous point, even with skew.
func TestEngineCommitSequence(t *testing.T) {
	tp := TopicPartition{Topic: "t", Partition: 0}
	// key a is slow: offsets 0(a),1(b),2(a) -> b finishes first.
	msgs := []Message{
		{Topic: "t", Partition: 0, Offset: 0, Key: []byte("a")},
		{Topic: "t", Partition: 0, Offset: 1, Key: []byte("b")},
		{Topic: "t", Partition: 0, Offset: 2, Key: []byte("a")},
	}
	be := newFakeBackend(msgs...)
	cfg := DefaultConfig()
	cfg.Lanes = 2
	cfg.CommitInterval = 5 * time.Millisecond
	handler := func(ctx context.Context, m *Message) error {
		if string(m.Key) == "a" {
			time.Sleep(30 * time.Millisecond)
		}
		return nil
	}
	c, err := New(be, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe([]string{"t"}, handler); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx)

	be.mu.Lock()
	defer be.mu.Unlock()
	if len(be.commits) == 0 {
		t.Fatal("no commits recorded")
	}
	last := be.commits[len(be.commits)-1]
	if off, ok := last[tp]; !ok {
		t.Fatalf("partition %v missing from final commit %+v", tp, last)
	} else if off != 3 {
		t.Fatalf("final committed offset = %d, want 3 (contiguous after all done)", off)
	}
}

// OnRevoked drains and commits final offsets.
func TestEngineRebalanceRevoke(t *testing.T) {
	tp := TopicPartition{Topic: "t", Partition: 0}
	be := newFakeBackend(
		Message{Topic: "t", Partition: 0, Offset: 0, Key: []byte("a")},
		Message{Topic: "t", Partition: 0, Offset: 1, Key: []byte("a")},
	)
	cfg := DefaultConfig()
	cfg.Lanes = 1
	handler := func(ctx context.Context, m *Message) error { return nil }
	c, err := New(be, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Subscribe([]string{"t"}, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go c.Run(ctx)

	// wait for assignment, then revoke
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		be.mu.Lock()
		assigned := be.h != nil
		be.mu.Unlock()
		if assigned {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	be.mu.Lock()
	h := be.h
	be.mu.Unlock()
	if h == nil {
		t.Fatal("rebalance handler not registered")
	}
	if err := h.OnRevoked(context.Background(), []TopicPartition{tp}); err != nil {
		t.Fatal(err)
	}
	c.Stop()
	<-time.After(20 * time.Millisecond)

	be.mu.Lock()
	defer be.mu.Unlock()
	if len(be.commits) == 0 {
		t.Fatal("no commit on revoke")
	}
	last := be.commits[len(be.commits)-1]
	if off, ok := last[tp]; !ok || off != 2 {
		t.Fatalf("revoke commit = %+v, want offset 2 for %v", last, tp)
	}
}

// backpressure pauses a partition at its in-flight cap.
func TestEngineBackpressure(t *testing.T) {
	tp := TopicPartition{Topic: "t", Partition: 0}
	cfg := DefaultConfig()
	cfg.Lanes = 2
	cfg.MaxInFlight = 2
	cfg.QueueSize = 16
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	handler := func(ctx context.Context, m *Message) error {
		once.Do(func() { close(started) })
		<-release // block all handlers
		return nil
	}
	be := newFakeBackend(
		Message{Topic: "t", Partition: 0, Offset: 0, Key: []byte("a")},
		Message{Topic: "t", Partition: 0, Offset: 1, Key: []byte("b")},
	)
	c, err := New(be, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Subscribe([]string{"t"}, handler)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go c.Run(ctx)

	select {
	case <-started:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("handler never started")
	}
	deadline := time.Now().Add(200 * time.Millisecond)
	paused := false
	for time.Now().Before(deadline) {
		be.mu.Lock()
		paused = be.paused[tp]
		be.mu.Unlock()
		if paused {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !paused {
		t.Fatal("partition was not paused at in-flight cap")
	}
	close(release)
	c.Stop()
}

// handler fatal error stops Run with the error.
func TestEngineFatalStops(t *testing.T) {
	be := newFakeBackend(Message{Topic: "t", Partition: 0, Offset: 0})
	cfg := DefaultConfig()
	cfg.Lanes = 1
	handler := func(ctx context.Context, m *Message) error { return fmt.Errorf("boom") }
	c, err := New(be, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Subscribe([]string{"t"}, handler)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = c.Run(ctx)
	if err == nil {
		t.Fatal("Run should return handler error")
	}
}

func TestStats(t *testing.T) {
	be := newFakeBackend(Message{Topic: "t", Partition: 0, Offset: 0})
	cfg := DefaultConfig()
	cfg.Lanes = 1
	handler := func(ctx context.Context, m *Message) error { return nil }
	c, err := New(be, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Subscribe([]string{"t"}, handler)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx)
	s := c.Stats()
	if s.Mode != KeyOrdered {
		t.Fatalf("stats mode = %v", s.Mode)
	}
}
