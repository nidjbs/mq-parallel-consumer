package engine

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// workerEnv bundles consumer-shared plumbing handed to each partition worker.
type workerEnv struct {
	fatalCh       chan error
	cancel        context.CancelFunc // cancels the consumer's work ctx on fatal
	inflightTotal *atomic.Int64
	counters      *counters
	advance       func() // notifies the committer that the tracker moved
}

// workResult is the outcome of a single handler attempt on a message.
type workResult int

const (
	workDone  workResult = iota // message finished (success or handled discard)
	workRetry                   // failed, retry configured; cool the chain head down
	workStop                    // fatal / shutdown; worker should stop
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
	highestSeen   atomic.Int64
	maxInFlight   int
	naturalCap    int // in-flight ceiling the route itself enforces (full lanes / semaphore)
	env           *workerEnv
	mu            sync.Mutex
	closed        bool
	pending       map[Offset]struct{} // routed but not yet completed
}

func newPartitionWorker(tp TopicPartition, cfg Config, env *workerEnv) *partitionWorker {
	w := &partitionWorker{
		tp:          tp,
		mode:        cfg.Mode,
		cfg:         cfg,
		tracker:     newOffsetTracker(),
		maxInFlight: cfg.MaxInFlight,
		env:         env,
		pending:     make(map[Offset]struct{}),
	}
	if cfg.Mode == Unordered {
		w.sem = make(chan struct{}, cfg.Concurrency)
		w.naturalCap = cfg.Concurrency
	} else {
		w.lanes = make([]*lane, cfg.Lanes)
		for i := range w.lanes {
			w.lanes[i] = newLane(i, w, cfg.QueueSize)
		}
		w.naturalCap = cfg.Lanes * cfg.QueueSize
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
	if o := int64(msg.Offset); o > w.highestSeen.Load() {
		w.highestSeen.Store(o)
	}

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
			defer w.env.inflightTotal.Add(-1)
			w.runUnordered(ctx, msg)
		}()
		return true
	}

	idx := w.laneIndex(msg)
	if !w.lanes[idx].push(ctx, msg) {
		w.forget(msg.Offset)
		w.inflight.Done()
		w.inflightCount.Add(-1)
		return false
	}
	return true
}

// keyOf returns the routing key of a message (empty when none).
func (w *partitionWorker) keyOf(msg *Message) string {
	if w.cfg.KeyExtractor != nil {
		return w.cfg.KeyExtractor(msg)
	}
	return string(msg.Key)
}

// laneIndex hashes the routing key to a lane; empty key -> lane 0.
func (w *partitionWorker) laneIndex(msg *Message) int {
	if len(w.lanes) <= 1 {
		return 0
	}
	key := w.keyOf(msg)
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
			l.closeLane()
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

// maxAttempts returns how many handler attempts a message may get.
func (w *partitionWorker) maxAttempts() int {
	if w.cfg.Retry.MaxAttempts <= 0 {
		return 1
	}
	return w.cfg.Retry.MaxAttempts
}

// runMessage attempts the handler once and decides the message's next step.
// Success (or a handled discard) advances the tracker and notifies the
// committer; a failure within MaxAttempts asks for a cooldown retry; exhaustion
// without OnDiscard reports fatal.
func (w *partitionWorker) runMessage(ctx context.Context, wk *work) (workResult, time.Duration) {
	err := w.safeHandler(ctx, wk.msg)
	if err == nil {
		w.env.counters.processed.Add(1)
		w.tracker.complete(wk.msg.Offset)
		w.finishOffset(wk.msg.Offset)
		return workDone, 0
	}
	if ctx.Err() != nil {
		w.finishOffset(wk.msg.Offset)
		return workStop, 0 // shutdown: leave offset uncommitted
	}
	if wk.attempts+1 < w.maxAttempts() {
		return workRetry, jitteredBackoff(retryBackoff(w.cfg.Retry, wk.attempts))
	}
	if w.cfg.OnDiscard != nil {
		if derr := w.safeOnDiscard(ctx, wk.msg, err); derr != nil {
			w.env.counters.handlerErrors.Add(1)
			w.finishOffset(wk.msg.Offset)
			w.reportFatal(fmt.Errorf("%w: %v", ErrHandlerFatal, derr))
			return workStop, 0
		}
		w.env.counters.discarded.Add(1)
		w.tracker.complete(wk.msg.Offset) // skipped: advance as completed
		w.finishOffset(wk.msg.Offset)
		return workDone, 0
	}
	w.env.counters.handlerErrors.Add(1)
	w.finishOffset(wk.msg.Offset)
	w.reportFatal(fmt.Errorf("%w: %v", ErrHandlerFatal, err))
	return workStop, 0
}

// runUnordered processes a single message with in-place backoff retries. Each
// message has its own goroutine, so its retries never block other messages.
func (w *partitionWorker) runUnordered(ctx context.Context, msg *Message) {
	wk := &work{msg: msg}
	for {
		res, backoff := w.runMessage(ctx, wk)
		switch res {
		case workDone, workStop:
			return
		case workRetry:
			wk.attempts++
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				w.finishOffset(msg.Offset)
				return
			}
		}
	}
}

// finishOffset clears the pending marker and notifies the committer that the
// tracker may have advanced.
func (w *partitionWorker) finishOffset(o Offset) {
	w.forget(o)
	if w.env.advance != nil {
		w.env.advance()
	}
}

// safeHandler runs the user handler and converts a panic into an error so a
// single bad message cannot crash the process.
func (w *partitionWorker) safeHandler(ctx context.Context, msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return w.handler(ctx, msg)
}

// safeOnDiscard runs the OnDiscard callback; a panic is returned as an error so
// the offset stays uncommitted and the consumer shuts down fatally.
func (w *partitionWorker) safeOnDiscard(ctx context.Context, msg *Message, handlerErr error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("OnDiscard panic: %v", r)
		}
	}()
	w.cfg.OnDiscard(ctx, msg, handlerErr)
	return nil
}

// forget removes an offset from the pending set.
func (w *partitionWorker) forget(o Offset) {
	w.mu.Lock()
	delete(w.pending, o)
	w.mu.Unlock()
}

// reportFatal delivers a fatal error to the poll loop and cancels the work
// context so a queue-blocked poll loop unwinds promptly.
func (w *partitionWorker) reportFatal(err error) {
	select {
	case w.env.fatalCh <- err:
	default:
	}
	if w.env.cancel != nil {
		w.env.cancel()
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
