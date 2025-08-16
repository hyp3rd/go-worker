package worker

// priorityQueue is a min-heap ordered by the provided less function.
// It is not safe for concurrent use; callers should synchronize access.
type priorityQueue[T any] struct {
	items    []T
	less     func(a, b T) bool
	setIndex func(T, int)
}

func newPriorityQueue[T any](less func(a, b T) bool, setIndex func(T, int)) *priorityQueue[T] {
	return &priorityQueue[T]{items: []T{}, less: less, setIndex: setIndex}
}

// Len returns the number of items in the queue.
func (pq priorityQueue[T]) Len() int { return len(pq.items) }

func (pq *priorityQueue[T]) Push(x T) {
	pq.items = append(pq.items, x)
	pq.setIndex(x, pq.Len()-1)
	pq.up(pq.Len() - 1)
}

// Pop removes and returns the highest priority item from the queue.
func (pq *priorityQueue[T]) Pop() (T, bool) {
	var zero T

	length := pq.Len()
	if length == 0 {
		return zero, false
	}

	pq.swap(0, length-1)
	item := pq.items[length-1]
	pq.items = pq.items[:length-1]
	pq.down(0)
	pq.setIndex(item, -1)

	return item, true
}

// Remove removes the item at the specified index from the queue.
func (pq *priorityQueue[T]) Remove(i int) (T, bool) {
	var zero T

	length := pq.Len() - 1
	if i < 0 || i > length {
		return zero, false
	}

	if i != length {
		pq.swap(i, length)
	}

	item := pq.items[length]

	pq.items = pq.items[:length]
	if i != length {
		pq.down(i)
		pq.up(i)
	}

	pq.setIndex(item, -1)

	return item, true
}

func (pq *priorityQueue[T]) swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.setIndex(pq.items[i], i)
	pq.setIndex(pq.items[j], j)
}

func (pq *priorityQueue[T]) lessIndex(i, j int) bool {
	return pq.less(pq.items[i], pq.items[j])
}

func (pq *priorityQueue[T]) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !pq.lessIndex(j, i) {
			break
		}

		pq.swap(i, j)
		j = i
	}
}

func (pq *priorityQueue[T]) down(i0 int) {
	length := pq.Len()

	i := i0
	for {
		left := 2*i + 1
		if left >= length {
			break
		}

		j := left

		right := left + 1
		if right < length && pq.lessIndex(right, left) {
			j = right
		}

		if !pq.lessIndex(j, i) {
			break
		}

		pq.swap(i, j)
		i = j
	}
}

// newTaskHeap returns a priority queue for *Task items ordered by priority.
func newTaskHeap() *priorityQueue[*Task] {
	return newPriorityQueue(func(a, b *Task) bool {
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}

		return a.ID.String() < b.ID.String()
	}, func(t *Task, i int) {
		t.index = i
	})
}
