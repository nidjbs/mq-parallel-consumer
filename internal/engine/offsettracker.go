package engine

import "sync"

// offsetTracker tracks completed offsets and advances a contiguous commit
// pointer. safe for concurrent use.
type offsetTracker struct {
	mu      sync.Mutex
	hasBase bool
	done    map[Offset]struct{} // completed but not yet past the contiguous run
	base    Offset              // next offset to commit; contiguous done = base-1
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{done: make(map[Offset]struct{})}
}

// seed initializes base from the first seen offset; later calls are no-ops.
func (t *offsetTracker) seed(o Offset) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.hasBase {
		t.base = o
		t.hasBase = true
	}
}

// complete marks o as done and returns the new base. Amortized O(1).
func (t *offsetTracker) complete(o Offset) Offset {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.hasBase {
		t.base = o
		t.hasBase = true
	}
	if o < t.base {
		return t.base // duplicate already committed
	}
	t.done[o] = struct{}{}
	for {
		if _, ok := t.done[t.base]; !ok {
			break
		}
		delete(t.done, t.base)
		t.base++
	}
	return t.base
}

// baseOffset returns the next offset to commit.
func (t *offsetTracker) baseOffset() Offset {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.base
}

// seeded reports whether the tracker has observed at least one offset.
func (t *offsetTracker) seeded() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.hasBase
}
