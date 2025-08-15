package worker

// taskHeap implements heap.Interface and holds Tasks ordered by priority.
// It is not safe for concurrent use; callers should synchronize access.
type taskHeap []*Task

func newTaskHeap() *taskHeap {
	h := taskHeap{}
	return &h
}

// Len returns the number of tasks in the heap.
func (th taskHeap) Len() int { return len(th) }

// Less reports whether the task with index i should sort before the task with index j.
// Tasks are ordered by Priority then by ID to provide a deterministic order.
func (th taskHeap) Less(i, j int) bool {
	if th[i].Priority != th[j].Priority {
		return th[i].Priority < th[j].Priority
	}
	return th[i].ID.String() < th[j].ID.String()
}

// Swap swaps the tasks with indexes i and j.
func (th taskHeap) Swap(i, j int) {
	th[i], th[j] = th[j], th[i]
	th[i].index = i
	th[j].index = j
}

// Push adds x as last element.
func (th *taskHeap) Push(x interface{}) {
	t := x.(*Task)
	*th = append(*th, t)
}

// Pop removes and returns the last element.
func (th *taskHeap) Pop() interface{} {
	old := *th
	n := len(old)
	if n == 0 {
		return nil
	}
	t := old[n-1]
	*th = old[0 : n-1]
	return t
}
