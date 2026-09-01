package swimlane

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Consumer struct {
	backend    Backend
	cfg        Config
	handler    Handler
	mu         sync.Mutex
	workers    map[TopicPartition]*partitionWorker
	paused     map[TopicPartition]bool
	stopCh     chan struct{}
	stopOnce   sync.Once
	workCtx    context.Context
	workCancel context.CancelFunc

	inflightTotal atomic.Int64
	fatalCh       chan error
}

// New validates the config and builds a consumer around a backend.
func New(backend Backend, cfg Config) (*Consumer, error) {
	if backend == nil {
		return nil, fmt.Errorf("%w: nil backend", ErrInvalidConfig)
	}
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Consumer{
		backend: backend,
		cfg:     cfg,
		workers: make(map[TopicPartition]*partitionWorker),
		paused:  make(map[TopicPartition]bool),
		stopCh:  make(chan struct{}),
		fatalCh: make(chan error, 1),
	}, nil
}

// Subscribe registers topics and the handler. May be called repeatedly.
func (c *Consumer) Subscribe(topics []string, handler Handler) error {
	c.mu.Lock()
	if c.handler == nil {
		c.handler = handler
	}
	c.mu.Unlock()
	return c.backend.Subscribe(topics)
}

// Run blocks until ctx is done, Stop is called, or a fatal error occurs.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.handler == nil {
		c.mu.Unlock()
		return fmt.Errorf("%w: Subscribe must be called before Run", ErrInvalidConfig)
	}
	c.workCtx, c.workCancel = context.WithCancel(ctx)
	c.mu.Unlock()

	c.backend.SetRebalanceHandler(c)

	var lastCommit time.Time
	first := true
	for {
		select {
		case <-ctx.Done():
			return c.shutdown(nil)
		case <-c.stopCh:
			return c.shutdown(nil)
		case err := <-c.fatalCh:
			return c.shutdown(err)
		default:
		}

		if c.cfg.CommitInterval <= 0 || first || time.Since(lastCommit) >= c.cfg.CommitInterval {
			c.commitAll(c.workCtx)
			lastCommit = time.Now()
			first = false
		}

		c.applyBackpressure(c.workCtx)

		msgs, err := c.backend.Poll(c.workCtx, c.cfg.PollTimeout)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return c.shutdown(nil)
			}
			return c.shutdown(err)
		}
		for i := range msgs {
			c.route(&msgs[i])
		}
	}
}

// Stop triggers graceful shutdown from any goroutine.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		c.mu.Lock()
		if c.workCancel != nil {
			c.workCancel()
		}
		c.mu.Unlock()
	})
}

// Stats returns a snapshot of the consumer state.
func (c *Consumer) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Stats{
		Mode:          c.cfg.Mode,
		Partitions:    len(c.workers),
		InFlightTotal: c.inflightTotal.Load(),
		PerPartition:  make(map[TopicPartition]PartitionStat, len(c.workers)),
	}
	for tp, w := range c.workers {
		s.PerPartition[tp] = PartitionStat{
			InFlight:    w.inFlight(),
			BaseOffset:  w.baseOffset(),
			MaxInFlight: w.maxInFlight,
		}
	}
	return s
}

// RebalanceHandler implementation -----------------------------------------

func (c *Consumer) OnAssigned(ctx context.Context, assigned map[TopicPartition]Offset) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for tp := range assigned {
		if _, ok := c.workers[tp]; ok {
			continue
		}
		w := newPartitionWorker(tp, c.cfg, c.fatalCh)
		w.setHandler(c.handler)
		w.start(c.workCtx)
		c.workers[tp] = w
	}
	return nil
}

func (c *Consumer) OnRevoked(ctx context.Context, revoked []TopicPartition) error {
	c.mu.Lock()
	victims := make([]*partitionWorker, 0, len(revoked))
	for _, tp := range revoked {
		if w, ok := c.workers[tp]; ok {
			victims = append(victims, w)
			delete(c.workers, tp)
		}
	}
	c.mu.Unlock()

	drainCtx, cancel := context.WithTimeout(ctx, c.cfg.RebalanceTimeout)
	defer cancel()
	commits := make(map[TopicPartition]Offset)
	for _, w := range victims {
		w.drain(drainCtx)
		if w.isSeeded() {
			commits[w.tp] = w.baseOffset()
		}
	}
	if len(commits) == 0 {
		return nil
	}
	return c.backend.Commit(drainCtx, commits)
}

// internals ------------------------------------------------------------------

func (c *Consumer) route(msg *Message) {
	c.mu.Lock()
	w, ok := c.workers[msg.TP()]
	c.mu.Unlock()
	if !ok {
		return // partition revoked between poll and route
	}
	c.inflightTotal.Add(1)
	if !w.route(c.workCtx, msg) {
		c.inflightTotal.Add(-1)
	}
}

func (c *Consumer) commitAll(ctx context.Context) {
	c.mu.Lock()
	commits := make(map[TopicPartition]Offset, len(c.workers))
	for tp, w := range c.workers {
		if w.isSeeded() {
			commits[tp] = w.baseOffset()
		}
	}
	c.mu.Unlock()
	if len(commits) == 0 {
		return
	}
	// v1: periodic commit errors are non-fatal, retried next tick
	_ = c.backend.Commit(ctx, commits)
}

func (c *Consumer) applyBackpressure(ctx context.Context) {
	c.mu.Lock()
	var pause, resume []TopicPartition
	for tp, w := range c.workers {
		n := w.inFlight()
		if n >= int64(w.maxInFlight) && !c.paused[tp] {
			pause = append(pause, tp)
			c.paused[tp] = true
		} else if n <= int64(float64(w.maxInFlight)*0.8) && c.paused[tp] {
			resume = append(resume, tp)
			delete(c.paused, tp)
		}
	}
	c.mu.Unlock()
	if len(pause) > 0 {
		_ = c.backend.Pause(pause)
	}
	if len(resume) > 0 {
		_ = c.backend.Resume(resume)
	}
}

func (c *Consumer) shutdown(finalErr error) error {
	c.mu.Lock()
	workers := make([]*partitionWorker, 0, len(c.workers))
	for _, w := range c.workers {
		workers = append(workers, w)
	}
	c.mu.Unlock()

	drainCtx, cancel := context.WithTimeout(context.Background(), c.cfg.RebalanceTimeout)
	defer cancel()
	commits := make(map[TopicPartition]Offset)
	for _, w := range workers {
		w.drain(drainCtx)
		if w.isSeeded() {
			commits[w.tp] = w.baseOffset()
		}
	}
	if len(commits) > 0 {
		if err := c.backend.Commit(drainCtx, commits); err != nil && finalErr == nil {
			finalErr = err
		}
	}
	_ = c.backend.Close(drainCtx)
	if finalErr != nil {
		return finalErr
	}
	return nil
}
