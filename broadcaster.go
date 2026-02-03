package worker

import (
	"sync"
	"sync/atomic"
)

// resultBroadcaster fans out results to multiple subscribers.
type resultBroadcaster struct {
	input   chan Result
	inputMu sync.Mutex
	mu      sync.RWMutex
	subs    map[uint64]chan Result
	nextID  uint64
	closed  atomic.Bool
	drops   atomic.Int64
	policy  atomic.Uint32
}

func newResultBroadcaster(buffer int) *resultBroadcaster {
	if buffer <= 0 {
		buffer = DefaultMaxTasks
	}

	broadcaster := &resultBroadcaster{
		input: make(chan Result, buffer),
		subs:  make(map[uint64]chan Result),
	}
	broadcaster.policy.Store(uint32(DropNewest))

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

	b.inputMu.Lock()
	defer b.inputMu.Unlock()

	if b.closed.Load() {
		return
	}

	b.input <- res
}

// Close shuts down the broadcaster and closes all subscriber channels.
func (b *resultBroadcaster) Close() {
	if b.closed.Swap(true) {
		return
	}

	b.inputMu.Lock()
	close(b.input)
	b.inputMu.Unlock()
}

func (b *resultBroadcaster) Drops() int64 {
	return b.drops.Load()
}

func (b *resultBroadcaster) SetDropPolicy(policy ResultDropPolicy) {
	switch policy {
	case DropNewest, DropOldest:
	default:
		policy = DropNewest
	}

	b.policy.Store(uint32(policy))
}

func (b *resultBroadcaster) dropPolicy() ResultDropPolicy {
	policy := b.policy.Load()
	if policy > uint32(DropOldest) {
		return DropNewest
	}

	return ResultDropPolicy(policy)
}

func (b *resultBroadcaster) loop() {
	for res := range b.input {
		policy := b.dropPolicy()

		b.mu.RLock()

		for _, ch := range b.subs {
			if !b.deliver(policy, ch, res) {
				b.drops.Add(1)
			}
		}

		b.mu.RUnlock()
	}

	b.mu.Lock()

	for id, ch := range b.subs {
		close(ch)
		delete(b.subs, id)
	}

	b.mu.Unlock()
}

func (*resultBroadcaster) deliver(policy ResultDropPolicy, ch chan Result, res Result) bool {
	//nolint:exhaustive
	switch policy {
	case DropOldest:
		select {
		case ch <- res:
			return true
		default:
		}

		select {
		case <-ch:
		default:
		}

		select {
		case ch <- res:
			return true
		default:
			return false
		}
	default:
		select {
		case ch <- res:
			return true
		default:
			return false
		}
	}
}
