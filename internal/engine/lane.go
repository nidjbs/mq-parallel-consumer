package engine

import (
	"context"
	"sync"
	"time"
)

// work is one message sitting on a per-key chain; attempts counts the failures
// already spent retrying it.
type work struct {
	msg      *Message
	attempts int
}

// keyChain queues the messages of one key FIFO. Only the head (queue[0]) is
// eligible, which keeps strict per-key ordering even while the head is cooling
// down after a failure.
type keyChain struct {
	queue   []*work
	retryAt time.Time // zero, or when the (re-inserted) head may be attempted again
}

// lane is one concurrency unit of KeyOrdered mode. Messages land on per-key
// chains; a single worker serves them. When the head of a key fails and retry
// is configured, the head is cooled down (retryAt) and the worker moves on to
// other keys in the same lane, so a retry never stalls unrelated keys. The
// total buffered messages across all chains is bounded by cfg.QueueSize via
// the slots semaphore.
type lane struct {
	idx int
	w   *partitionWorker

	mu        sync.Mutex
	wake      chan struct{} // cap 1; push / close / space-freeing notifications
	closed    bool          // worker must finish current work then exit
	accepting bool          // false after close: new pushes are rejected
	slots     chan struct{} // cap QueueSize; taken on push, released on final removal
	chains    map[string]*keyChain
}

func newLane(idx int, w *partitionWorker, size int) *lane {
	return &lane{
		idx:       idx,
		w:         w,
		wake:      make(chan struct{}, 1),
		accepting: true,
		slots:     make(chan struct{}, size),
		chains:    make(map[string]*keyChain),
	}
}

// push queues msg, blocking while the lane is full. Returns false when the
// lane is closed or ctx is done (caller keeps ownership of the slot bookkeeping
// already done upstream).
func (l *lane) push(ctx context.Context, msg *Message) bool {
	select {
	case l.slots <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	l.mu.Lock()
	if !l.accepting {
		l.mu.Unlock()
		<-l.slots // give the slot back
		return false
	}
	key := l.w.keyOf(msg)
	c := l.chains[key]
	if c == nil {
		c = &keyChain{}
		l.chains[key] = c
	}
	c.queue = append(c.queue, &work{msg: msg})
	l.mu.Unlock()
	l.signal()
	return true
}

func (l *lane) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// closeLane stops accepting pushes and asks the worker to finish then exit;
// messages still queued but never started are abandoned (re-consumed later).
func (l *lane) closeLane() {
	l.mu.Lock()
	l.accepting = false
	l.closed = true
	l.mu.Unlock()
	l.signal()
}

// run is the single worker loop: pick a processable chain head, run it, then
// arrange the head per the outcome until the lane closes or the context ends.
func (l *lane) run(ctx context.Context) {
	for {
		wk := l.takeNext(ctx)
		if wk == nil {
			l.abandonAll()
			return
		}
		if stop := l.runWork(ctx, wk); stop {
			l.abandonAll()
			return
		}
	}
}

// runWork runs one message and updates its chain. Returns true when the worker
// must stop (fatal error or context shutdown).
func (l *lane) runWork(ctx context.Context, wk *work) bool {
	res, backoff := l.w.runMessage(ctx, wk)
	switch res {
	case workDone:
		l.finishHead(wk)
		return false
	case workRetry:
		wk.attempts++
		l.cooldownHead(wk, backoff)
		return false
	default: // workStop
		l.dropHead(wk)
		return true
	}
}

// takeNext removes the head of a processable chain and returns it. It blocks
// until such work exists, a cooling head becomes due, or ctx ends (returning
// nil). Once closed, the worker still drains cooling heads so a graceful
// shutdown finishes short retries instead of abandoning them.
func (l *lane) takeNext(ctx context.Context) *work {
	l.mu.Lock()
	for {
		if wk := l.pickLocked(); wk != nil {
			l.mu.Unlock()
			return wk
		}
		if !l.hasQueuedLocked() {
			// nothing left to process: only wait for new pushes while open
			if l.closed {
				l.mu.Unlock()
				return nil
			}
			l.mu.Unlock()
			select {
			case <-l.wake:
			case <-ctx.Done():
				return nil
			}
			l.mu.Lock()
			continue
		}
		// queued messages exist but every head is cooling down
		if next := l.earliestRetryLocked(); !next.IsZero() {
			if wait := time.Until(next); wait <= 0 {
				continue // already due; pickLocked will take it on the next pass
			} else {
				l.mu.Unlock()
				select {
				case <-l.wake:
				case <-time.After(wait):
				case <-ctx.Done():
					return nil
				}
				l.mu.Lock()
				continue
			}
		}
		l.mu.Unlock()
		select {
		case <-l.wake:
		case <-ctx.Done():
			return nil
		}
		l.mu.Lock()
	}
}

// hasQueuedLocked reports whether any chain still holds a message.
func (l *lane) hasQueuedLocked() bool {
	for _, c := range l.chains {
		if len(c.queue) > 0 {
			return true
		}
	}
	return false
}

// pickLocked returns the head of a chain whose cooldown has passed, removing it
// from the chain and clearing the cooldown for the next head. Called with l.mu.
func (l *lane) pickLocked() *work {
	for key, c := range l.chains {
		if len(c.queue) == 0 {
			delete(l.chains, key)
			continue
		}
		if !c.retryAt.IsZero() && c.retryAt.After(time.Now()) {
			continue // head still cooling down
		}
		c.retryAt = time.Time{}
		wk := c.queue[0]
		c.queue = c.queue[1:]
		return wk
	}
	return nil
}

// earliestRetryLocked returns the soonest cooling deadline among chains.
func (l *lane) earliestRetryLocked() time.Time {
	var e time.Time
	for _, c := range l.chains {
		if len(c.queue) > 0 && !c.retryAt.IsZero() && (e.IsZero() || c.retryAt.Before(e)) {
			e = c.retryAt
		}
	}
	return e
}

// finishHead finalizes a successfully processed message (already dequeued).
func (l *lane) finishHead(wk *work) {
	l.removeChainIfEmpty(wk)
	l.releaseSlot()
	l.markFinalized()
	l.signal()
}

// cooldownHead puts a failed message back at the head of its chain until the
// given backoff elapses; the slot is kept because the message stays buffered.
func (l *lane) cooldownHead(wk *work, backoff time.Duration) {
	l.mu.Lock()
	key := l.w.keyOf(wk.msg)
	c := l.chains[key]
	if c == nil {
		// chain vanished while the head was out; treat as abandoned
		l.mu.Unlock()
		l.dropHead(wk)
		return
	}
	c.queue = append([]*work{wk}, c.queue...)
	c.retryAt = time.Now().Add(backoff)
	l.mu.Unlock()
}

// dropHead discards a message that reached a terminal failure (fatal or
// shutdown); its offset stays uncommitted so the next owner re-consumes it.
func (l *lane) dropHead(wk *work) {
	l.removeChainIfEmpty(wk)
	l.releaseSlot()
	l.markFinalized()
	l.signal()
}

// removeChainIfEmpty drops the chain of wk when it holds no more messages; the
// dequeue already happened in takeNext. Called without l.mu.
func (l *lane) removeChainIfEmpty(wk *work) {
	l.mu.Lock()
	if c, ok := l.chains[l.w.keyOf(wk.msg)]; ok && len(c.queue) == 0 {
		delete(l.chains, l.w.keyOf(wk.msg))
	}
	l.mu.Unlock()
}

// abandonAll drops every message still queued across the chains (the lane is
// closing); offsets are left uncommitted for re-consumption.
func (l *lane) abandonAll() {
	l.mu.Lock()
	if len(l.chains) == 0 {
		l.mu.Unlock()
		return
	}
	var all []*work
	for key, c := range l.chains {
		all = append(all, c.queue...)
		delete(l.chains, key)
	}
	l.mu.Unlock()
	for _, wk := range all {
		l.w.forget(wk.msg.Offset)
		l.releaseSlot()
		l.markFinalized()
	}
}

func (l *lane) releaseSlot() {
	<-l.slots
}

// markFinalized mirrors the per-message accounting done when a message leaves
// the lane (success, terminal failure, or abandonment).
func (l *lane) markFinalized() {
	l.w.env.inflightTotal.Add(-1)
	l.w.inflightCount.Add(-1)
	l.w.inflight.Done()
}
