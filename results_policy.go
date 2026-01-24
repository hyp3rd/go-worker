package worker

// ResultDropPolicy defines how to handle full subscriber buffers.
type ResultDropPolicy uint8

const (
	// DropNewest drops the new result when the subscriber buffer is full.
	DropNewest ResultDropPolicy = iota
	// DropOldest drops the oldest buffered result to make room for the new one.
	DropOldest
)
