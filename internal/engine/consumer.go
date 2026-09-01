package engine

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

	runState     atomic.Bool // single-use guard: Run may only be called once
	rebalanceErr error       // revoke commit failure, surfaced by Run
	counters     *counters

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
		backend:  backend,
		cfg:      cfg,
		workers:  make(map[TopicPartition]*partitionWorker),
		paused:   make(map[TopicPartition]bool),
		stopCh:   make(chan struct{}),
		fatalCh:  make(chan error, 1),
		counters: &counters{},
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
// A consumer is single-use: Run may be called at most once.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	noHandler := c.handler == nil
	c.mu.Unlock()
	if noHandler {
		return fmt.Errorf("%w: Subscribe must be called before Run", ErrInvalidConfig)
	}
	if !c.runState.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: Run called more than once", ErrAlreadyRunning)
	}
	c.mu.Lock()
	c.workCtx, c.workCancel = context.WithCancel(ctx)
	c.mu.Unlock()

	c.backend.SetRebalanceHandler(c)

	var lastCommit time.Time
	first := true
	for {
		if rerr := c.takeRebalanceErr(); rerr != nil {
			return c.shutdown(rerr)
		}
		select {
		case <-ctx.Done():
			return c.shutdown(nil)
		case <-c.stopCh:
			return c.shutdown(nil)
		case err := <-c.fatalCh:
			return c.shutdown(err)
		default:
		}

		if c.cfg.CommitInterval > 0 {
			if first || time.Since(lastCommit) >= c.cfg.CommitInterval {
				c.commitAll(c.workCtx, false)
				lastCommit = time.Now()
				first = false
			}
		} else {
			c.commitAll(c.workCtx, true) // commit-on-advance
		}

		c.applyBackpressure(c.workCtx)

		msgs, err := c.backend.Poll(c.workCtx, c.cfg.PollTimeout)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return c.shutdown(nil)
			}
			if errors.Is(err, context.DeadlineExceeded) {
				if c.workCtx.Err() == nil {
					continue // poll window elapsed with nothing to consume; backend healthy
				}
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
		Mode:              c.cfg.Mode,
		Partitions:        len(c.workers),
		InFlightTotal:     c.inflightTotal.Load(),
		PerPartition:      make(map[TopicPartition]PartitionStat, len(c.workers)),
		MessagesProcessed: c.counters.processed.Load(),
		MessagesDiscarded: c.counters.discarded.Load(),
		HandlerErrors:     c.counters.handlerErrors.Load(),
		Commits:           c.counters.commits.Load(),
		CommitErrors:      c.counters.commitErrors.Load(),
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
		w := newPartitionWorker(tp, c.cfg, &workerEnv{
			fatalCh:       c.fatalCh,
			cancel:        c.workCancel,
			inflightTotal: &c.inflightTotal,
			counters:      c.counters,
		})
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
		delete(c.paused, tp) // stale pause bookkeeping from an earlier assignment
		if w, ok := c.workers[tp]; ok {
			victims = append(victims, w)
			delete(c.workers, tp)
		}
	}
	c.mu.Unlock()

	// drain and final commit get independent budgets: a slow drain must not
	// leave the commit with an already-expired context.
	drainCtx, drainCancel := context.WithTimeout(ctx, c.cfg.RebalanceTimeout)
	commits := make(map[TopicPartition]Offset)
	for _, w := range victims {
		w.drain(drainCtx)
		if w.isSeeded() {
			commits[w.tp] = w.baseOffset()
		}
	}
	drainCancel()
	// best-effort: clear transport-level pause so a later reassignment of the
	// same partition to this client does not start out paused.
	if len(revoked) > 0 {
		_ = c.backend.Resume(revoked)
	}
	if len(commits) == 0 {
		return nil
	}
	commitCtx, commitCancel := context.WithTimeout(ctx, c.cfg.RebalanceTimeout)
	defer commitCancel()
	if err := c.backend.Commit(commitCtx, commits); err != nil {
		c.mu.Lock()
		if c.rebalanceErr == nil {
			c.rebalanceErr = err
		}
		c.mu.Unlock()
		return err
	}
	return nil
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

// commitAll commits each seeded partition's base offset. With advanceOnly set
// (commit-on-advance mode) a partition is skipped unless its base moved past
// the last successful commit. Errors are non-fatal and retried next tick.
func (c *Consumer) commitAll(ctx context.Context, advanceOnly bool) {
	c.mu.Lock()
	commits := make(map[TopicPartition]Offset, len(c.workers))
	for tp, w := range c.workers {
		if !w.isSeeded() {
			continue
		}
		if advanceOnly && !w.tracker.advancedSinceCommit() {
			continue
		}
		commits[tp] = w.baseOffset()
	}
	c.mu.Unlock()
	if len(commits) == 0 {
		return
	}
	c.counters.commits.Add(1)
	if err := c.backend.Commit(ctx, commits); err != nil {
		c.counters.commitErrors.Add(1)
		return
	}
	c.mu.Lock()
	for tp := range commits {
		if w, ok := c.workers[tp]; ok {
			w.tracker.markCommitted()
		}
	}
	c.mu.Unlock()
}

// takeRebalanceErr returns and clears a pending revoke-commit failure.
func (c *Consumer) takeRebalanceErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.rebalanceErr
	c.rebalanceErr = nil
	return err
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
	// A work-context cancel can make Poll return context.Canceled before the
	// poll loop reaches the fatalCh select; prefer a real fatal over a clean
	// shutdown.
	if finalErr == nil {
		select {
		case err := <-c.fatalCh:
			finalErr = err
		default:
		}
	}
	c.mu.Lock()
	workers := make([]*partitionWorker, 0, len(c.workers))
	for _, w := range c.workers {
		workers = append(workers, w)
	}
	if c.workCancel != nil {
		c.workCancel()
	}
	c.mu.Unlock()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), c.cfg.RebalanceTimeout)
	commits := make(map[TopicPartition]Offset)
	for _, w := range workers {
		w.drain(drainCtx)
		if w.isSeeded() {
			commits[w.tp] = w.baseOffset()
		}
	}
	drainCancel()
	if len(commits) > 0 {
		// fresh budget: a slow drain must not expire the final commit's context
		commitCtx, commitCancel := context.WithTimeout(context.Background(), c.cfg.RebalanceTimeout)
		defer commitCancel()
		if err := c.backend.Commit(commitCtx, commits); err != nil && finalErr == nil {
			finalErr = err
		}
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), c.cfg.RebalanceTimeout)
	defer closeCancel()
	_ = c.backend.Close(closeCtx)
	if finalErr != nil {
		return finalErr
	}
	return nil
}
