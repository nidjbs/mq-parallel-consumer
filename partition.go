package swimlane

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// partitionWorker owns one partition's lanes and offset tracker.
type partitionWorker struct {
	tp            TopicPartition
	mode          Mode
	cfg           Config
	handler       Handler
	lanes         []*lane
	sem           chan struct{}
	tracker       *offsetTracker
	inflight      sync.WaitGroup
	inflightCount atomic.Int64
	maxInFlight   int
	fatalCh       chan error
	mu            sync.Mutex
	closed        bool
	pending       map[Offset]struct{} // routed but not yet completed
}

func newPartitionWorker(tp TopicPartition, cfg Config, fatalCh chan error) *partitionWorker {
	w := &partitionWorker{
		tp:          tp,
		mode:        cfg.Mode,
		cfg:         cfg,
		tracker:     newOffsetTracker(),
		maxInFlight: cfg.MaxInFlight,
		fatalCh:     fatalCh,
		pending:     make(map[Offset]struct{}),
	}
	if cfg.Mode == Unordered {
		w.sem = make(chan struct{}, cfg.Concurrency)
	} else {
		w.lanes = make([]*lane, cfg.Lanes)
		for i := range w.lanes {
			w.lanes[i] = newLane(i, w, cfg.QueueSize)
		}
	}
	return w
}

// setHandler injects the message handler (set by the consumer on assignment).
func (w *partitionWorker) setHandler(h Handler) {
	w.handler = h
}

// start launches lane workers (no-op for Unordered).
func (w *partitionWorker) start(ctx context.Context) {
	if w.mode == KeyOrdered {
		for _, l := range w.lanes {
			go l.run(ctx)
		}
	}
}

// route hands a message to the partition; returns false if dropped.
// Must be called from the poll goroutine (never concurrent with drain).
func (w *partitionWorker) route(ctx context.Context, msg *Message) bool {
	w.tracker.seed(msg.Offset)
	if msg.Offset < w.tracker.baseOffset() {
		return true // already committed, skip
	}

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return false
	}
	if _, dup := w.pending[msg.Offset]; dup {
		w.mu.Unlock()
		return true // duplicate already routed, skip
	}
	w.pending[msg.Offset] = struct{}{}
	w.mu.Unlock()

	w.inflight.Add(1)
	w.inflightCount.Add(1)
	if w.mode == Unordered {
		select {
		case w.sem <- struct{}{}:
		case <-ctx.Done():
			w.inflight.Done()
			w.inflightCount.Add(-1)
			return false
		}
		go func() {
			defer func() { <-w.sem }()
			defer w.inflight.Done()
			defer w.inflightCount.Add(-1)
			w.handle(ctx, msg)
		}()
		return true
	}

	idx := w.laneIndex(msg)
	select {
	case w.lanes[idx].q <- msg:
		return true
	case <-ctx.Done():
		w.inflight.Done()
		w.inflightCount.Add(-1)
		return false
	}
}

// laneIndex hashes the extracted key to a lane; empty key -> lane 0.
func (w *partitionWorker) laneIndex(msg *Message) int {
	if len(w.lanes) <= 1 {
		return 0
	}
	var key string
	if w.cfg.KeyExtractor != nil {
		key = w.cfg.KeyExtractor(msg)
	} else {
		key = string(msg.Key)
	}
	if key == "" {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(w.lanes)))
}

// drain closes lanes and waits for in-flight work with ctx timeout.
func (w *partitionWorker) drain(ctx context.Context) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	lanes := w.lanes
	w.mu.Unlock()

	if w.mode == KeyOrdered {
		for _, l := range lanes {
			close(l.q)
		}
	}
	done := make(chan struct{})
	go func() {
		w.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		// timeout: force stop; unfinished messages re-consumed by new owner
	}
}

// handle processes a message and advances the tracker, or reports fatal.
func (w *partitionWorker) handle(ctx context.Context, msg *Message) {
	defer w.forget(msg.Offset)
	err := w.process(ctx, msg)
	if err == nil {
		w.tracker.complete(msg.Offset)
		return
	}
	if ctx.Err() != nil {
		return // shutdown/drain: leave offset uncommitted
	}
	w.reportFatal(fmt.Errorf("%w: %v", ErrHandlerFatal, err))
}

// process runs the handler with retry, then OnDiscard if configured.
func (w *partitionWorker) process(ctx context.Context, msg *Message) error {
	maxAttempts := w.cfg.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		lastErr = w.handler(ctx, msg)
		if lastErr == nil {
			return nil
		}
		if attempt == maxAttempts-1 {
			break
		}
		backoff := retryBackoff(w.cfg.Retry, attempt)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if w.cfg.OnDiscard != nil {
		w.cfg.OnDiscard(ctx, msg, lastErr)
		return nil
	}
	return lastErr
}

// forget removes an offset from the pending set.
func (w *partitionWorker) forget(o Offset) {
	w.mu.Lock()
	delete(w.pending, o)
	w.mu.Unlock()
}

// reportFatal delivers a fatal error to the poll loop (non-blocking).
func (w *partitionWorker) reportFatal(err error) {
	select {
	case w.fatalCh <- err:
	default:
	}
}

func (w *partitionWorker) baseOffset() Offset {
	return w.tracker.baseOffset()
}

func (w *partitionWorker) inFlight() int64 {
	return w.inflightCount.Load()
}

func (w *partitionWorker) isSeeded() bool {
	return w.tracker.seeded()
}
