package worker

import (
	"sync"
	"sync/atomic"
)

// resultBroadcaster fans out results to multiple subscribers.
type resultBroadcaster struct {
	input  chan Result
	mu     sync.Mutex
	subs   map[uint64]chan Result
	nextID uint64
	closed atomic.Bool
	drops  atomic.Int64
}

func newResultBroadcaster(buffer int) *resultBroadcaster {
	if buffer <= 0 {
		buffer = DefaultMaxTasks
	}

	broadcaster := &resultBroadcaster{
		input: make(chan Result, buffer),
		subs:  make(map[uint64]chan Result),
	}

	go broadcaster.loop()

	return broadcaster
}

// Subscribe returns a channel and an unsubscribe function.
func (b *resultBroadcaster) Subscribe(buffer int) (<-chan Result, func()) {
	if buffer <= 0 {
		buffer = 1
	}

	ch := make(chan Result, buffer)

	b.mu.Lock()

	if b.closed.Load() {
		b.mu.Unlock()
		close(ch)

		return ch, func() {}
	}

	id := b.nextID
	b.nextID++
	b.subs[id] = ch
	b.mu.Unlock()

	unsubscribe := func() {
		b.mu.Lock()

		if existing, ok := b.subs[id]; ok {
			delete(b.subs, id)
			close(existing)
		}

		b.mu.Unlock()
	}

	return ch, unsubscribe
}

// Publish sends a result to subscribers.
func (b *resultBroadcaster) Publish(res Result) {
	if b.closed.Load() {
		return
	}

	defer func() {
		err := recover()
		if err != nil {
			// broadcaster is closed
			b.drops.Add(1)
		}
	}()

	b.input <- res
}

// Close shuts down the broadcaster and closes all subscriber channels.
func (b *resultBroadcaster) Close() {
	if b.closed.Swap(true) {
		return
	}

	close(b.input)
}

func (b *resultBroadcaster) Drops() int64 {
	return b.drops.Load()
}

func (b *resultBroadcaster) loop() {
	for res := range b.input {
		subs := b.snapshot()
		for _, ch := range subs {
			select {
			case ch <- res:
			default:
				b.drops.Add(1)
			}
		}
	}

	b.mu.Lock()

	for id, ch := range b.subs {
		close(ch)
		delete(b.subs, id)
	}

	b.mu.Unlock()
}

func (b *resultBroadcaster) snapshot() []chan Result {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs := make([]chan Result, 0, len(b.subs))
	for _, ch := range b.subs {
		subs = append(subs, ch)
	}

	return subs
}
