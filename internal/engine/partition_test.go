package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestConfig() Config {
	c := DefaultConfig()
	c.Lanes = 3
	c.QueueSize = 16
	return c
}

// newTestWorker builds a worker with the given handler and starts it.
func newTestWorker(cfg Config, h Handler) *partitionWorker {
	env := &workerEnv{
		fatalCh:       make(chan error, 1),
		inflightTotal: new(atomic.Int64),
		counters:      new(counters),
	}
	w := newPartitionWorker(TopicPartition{Topic: "t", Partition: 0}, cfg, env)
	w.setHandler(h)
	w.start(context.Background())
	return w
}

// same key always goes to the same lane; per-key order preserved.
func TestKeyedLaneOrdering(t *testing.T) {
	cfg := newTestConfig()
	var mu sync.Mutex
	perKey := map[string][]int64{}
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error {
		mu.Lock()
		perKey[string(m.Key)] = append(perKey[string(m.Key)], int64(m.Offset))
		mu.Unlock()
		return nil
	})

	msgs := []*Message{
		{Key: []byte("a"), Offset: 0},
		{Key: []byte("b"), Offset: 1},
		{Key: []byte("a"), Offset: 2},
		{Key: []byte("c"), Offset: 3},
		{Key: []byte("b"), Offset: 4},
		{Key: []byte("a"), Offset: 5},
	}
	for _, m := range msgs {
		w.route(context.Background(), m)
	}
	w.drain(context.Background())

	want := map[string][]int64{
		"a": {0, 2, 5},
		"b": {1, 4},
		"c": {3},
	}
	if len(perKey) != len(want) {
		t.Fatalf("perKey = %+v", perKey)
	}
	for k, seq := range want {
		got, ok := perKey[k]
		if !ok {
			t.Fatalf("key %q missing", k)
		}
		for i := range seq {
			if got[i] != seq[i] {
				t.Fatalf("key %q seq = %v, want %v", k, got, seq)
			}
		}
	}
	// contiguous base must advance to 6
	if got := w.baseOffset(); got != 6 {
		t.Fatalf("base = %d, want 6", got)
	}
}

// empty key routes to lane 0.
func TestEmptyKeyLaneZero(t *testing.T) {
	cfg := newTestConfig()
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return nil })
	if idx := w.laneIndex(&Message{Key: nil}); idx != 0 {
		t.Fatalf("empty key idx = %d, want 0", idx)
	}
	w.drain(context.Background())
}

// retry then success: base advances; handler called MaxAttempts times.
func TestRetryThenSuccess(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond}
	var calls atomic.Int32
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error {
		if calls.Add(1) < 3 {
			return context.DeadlineExceeded // transient failure
		}
		return nil
	})
	w.route(context.Background(), &Message{Offset: 0})
	w.drain(context.Background())

	if calls.Load() != 3 {
		t.Fatalf("handler calls = %d, want 3", calls.Load())
	}
	if got := w.baseOffset(); got != 1 {
		t.Fatalf("base = %d, want 1", got)
	}
}

// retry exhausted + OnDiscard: base advances, no fatal.
func TestRetryExhaustedOnDiscard(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}
	cfg.OnDiscard = func(ctx context.Context, m *Message, err error) {}
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return context.DeadlineExceeded })
	w.route(context.Background(), &Message{Offset: 0})
	w.drain(context.Background())

	if got := w.baseOffset(); got != 1 {
		t.Fatalf("base = %d, want 1", got)
	}
}

// retry exhausted + no OnDiscard: fatal, base does NOT advance.
func TestRetryExhaustedFatal(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 1, InitialBackoff: time.Millisecond}
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return context.DeadlineExceeded })
	w.route(context.Background(), &Message{Offset: 0})
	w.drain(context.Background())

	if got := w.baseOffset(); got != 0 {
		t.Fatalf("base = %d, want 0 (uncommitted)", got)
	}
	select {
	case <-w.env.fatalCh:
	default:
		t.Fatal("expected fatal error")
	}
}

// duplicate offset (< base) is skipped without processing.
func TestDuplicateSkipped(t *testing.T) {
	cfg := newTestConfig()
	var calls atomic.Int32
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error {
		calls.Add(1)
		return nil
	})
	w.route(context.Background(), &Message{Offset: 0})
	w.route(context.Background(), &Message{Offset: 0}) // duplicate
	w.route(context.Background(), &Message{Offset: 1})
	w.drain(context.Background())

	if calls.Load() != 2 {
		t.Fatalf("handler calls = %d, want 2", calls.Load())
	}
}

// Unordered mode: all messages processed, base advances.
func TestUnorderedMode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Mode = Unordered
	cfg.Concurrency = 4
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return nil })
	for i := 0; i < 20; i++ {
		w.route(context.Background(), &Message{Offset: Offset(i)})
	}
	w.drain(context.Background())
	if got := w.baseOffset(); got != 20 {
		t.Fatalf("base = %d, want 20", got)
	}
}

// a failing head that enters cooldown must not stall other keys on the lane:
// fast key finishes during slow key's backoff.
func TestCooldownDoesNotBlockOtherKeys(t *testing.T) {
	cfg := newTestConfig()
	cfg.Lanes = 1
	cfg.QueueSize = 16
	cfg.Retry = RetryPolicy{MaxAttempts: 2, InitialBackoff: 50 * time.Millisecond}
	var slowCalls, fastCalls atomic.Int32
	var fastDoneAt, slowSecondAt atomic.Int64
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error {
		if string(m.Key) == "slow" {
			if slowCalls.Add(1) == 1 {
				return errors.New("transient")
			}
			slowSecondAt.Store(time.Now().UnixMilli())
			return nil
		}
		fastCalls.Add(1)
		fastDoneAt.Store(time.Now().UnixMilli())
		return nil
	})
	w.route(context.Background(), &Message{Key: []byte("slow"), Offset: 0})
	w.route(context.Background(), &Message{Key: []byte("fast"), Offset: 1})
	w.drain(context.Background())

	if slowCalls.Load() != 2 || fastCalls.Load() != 1 {
		t.Fatalf("calls: slow=%d (want 2), fast=%d (want 1)", slowCalls.Load(), fastCalls.Load())
	}
	if fastDoneAt.Load() >= slowSecondAt.Load() {
		t.Fatalf("fast finished %dms after slow's retry: cooldown blocked other keys",
			slowSecondAt.Load()-fastDoneAt.Load())
	}
	if got := w.baseOffset(); got != 2 {
		t.Fatalf("base = %d, want 2", got)
	}
}

// per-key order survives a retry: the next message of the same key runs only
// after the failed head has been retried to success.
func TestKeyOrderPreservedAcrossRetry(t *testing.T) {
	cfg := newTestConfig()
	cfg.Lanes = 1
	cfg.Retry = RetryPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond}
	var calls0, calls1 atomic.Int32
	var done0At, done1At atomic.Int64
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error {
		if m.Offset == 0 {
			if calls0.Add(1) == 1 {
				return errors.New("transient")
			}
			done0At.Store(time.Now().UnixMilli())
			return nil
		}
		calls1.Add(1)
		done1At.Store(time.Now().UnixMilli())
		return nil
	})
	w.route(context.Background(), &Message{Key: []byte("k"), Offset: 0})
	w.route(context.Background(), &Message{Key: []byte("k"), Offset: 1})
	w.drain(context.Background())

	if calls0.Load() != 2 || calls1.Load() != 1 {
		t.Fatalf("calls: offset0=%d (want 2), offset1=%d (want 1)", calls0.Load(), calls1.Load())
	}
	if done1At.Load() < done0At.Load() {
		t.Fatalf("offset1 ran before offset0's retry finished: per-key order broken")
	}
	if got := w.baseOffset(); got != 2 {
		t.Fatalf("base = %d, want 2", got)
	}
}

// highestSeen tracks the furthest routed offset regardless of completion.
func TestHighestSeenTracked(t *testing.T) {
	cfg := newTestConfig()
	cfg.Lanes = 3
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error {
		if string(m.Key) == "a" {
			once.Do(func() { close(started) })
			<-release // hold offset 0 open
		}
		return nil
	})
	w.route(context.Background(), &Message{Key: []byte("a"), Offset: 0})
	<-started // offset 0 in flight; base can never pass the hole at 0
	w.route(context.Background(), &Message{Key: []byte("b"), Offset: 1})
	w.route(context.Background(), &Message{Key: []byte("c"), Offset: 2})

	if got := Offset(w.highestSeen.Load()); got != 2 {
		t.Fatalf("HighestSeen = %d, want 2", got)
	}
	if got := w.baseOffset(); got != 0 {
		t.Fatalf("base = %d, want 0 (hole at 0)", got)
	}
	close(release)
	w.drain(context.Background())
	if got := w.baseOffset(); got != 3 {
		t.Fatalf("base after drain = %d, want 3", got)
	}
}

// drain is idempotent.
func TestDrainIdempotent(t *testing.T) {
	cfg := newTestConfig()
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return nil })
	w.route(context.Background(), &Message{Offset: 0})
	w.drain(context.Background())
	w.drain(context.Background())
}

// a panic in the handler becomes a fatal error instead of crashing the process.
func TestHandlerPanicBecomesFatal(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 1}
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { panic("boom") })
	w.route(context.Background(), &Message{Offset: 0})
	w.drain(context.Background())

	select {
	case err := <-w.env.fatalCh:
		if !strings.Contains(err.Error(), "handler panic") {
			t.Fatalf("fatal = %v, want handler panic", err)
		}
	default:
		t.Fatal("expected fatal error from handler panic")
	}
	if got := w.baseOffset(); got != 0 {
		t.Fatalf("base = %d, want 0 (uncommitted)", got)
	}
}

// a panic in OnDiscard is fatal and leaves the offset uncommitted.
func TestOnDiscardPanicFatal(t *testing.T) {
	cfg := newTestConfig()
	cfg.Retry = RetryPolicy{MaxAttempts: 1}
	cfg.OnDiscard = func(ctx context.Context, m *Message, err error) { panic("discard boom") }
	w := newTestWorker(cfg, func(ctx context.Context, m *Message) error { return errors.New("fail") })
	w.route(context.Background(), &Message{Offset: 0})
	w.drain(context.Background())

	select {
	case err := <-w.env.fatalCh:
		if !strings.Contains(err.Error(), "OnDiscard panic") {
			t.Fatalf("fatal = %v, want OnDiscard panic", err)
		}
	default:
		t.Fatal("expected fatal error from OnDiscard panic")
	}
	if got := w.baseOffset(); got != 0 {
		t.Fatalf("base = %d, want 0 (uncommitted)", got)
	}
}
