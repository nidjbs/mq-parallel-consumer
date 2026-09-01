package engine

import "context"

type lane struct {
	idx int
	q   chan *Message
	w   *partitionWorker
}

func newLane(idx int, w *partitionWorker, size int) *lane {
	return &lane{idx: idx, q: make(chan *Message, size), w: w}
}

// run drains the queue serially until the channel is closed.
func (l *lane) run(ctx context.Context) {
	for msg := range l.q {
		l.w.handle(ctx, msg)
		l.w.inflight.Done()
		l.w.inflightCount.Add(-1)
	}
}
