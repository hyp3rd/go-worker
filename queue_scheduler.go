package worker

import "sort"

type taskQueue struct {
	name      string
	weight    int
	remaining int
	tasks     *priorityQueue[*Task]
}

type queueScheduler struct {
	queues map[string]*taskQueue
	order  []string
	total  int
	cursor int
}

func newQueueScheduler() *queueScheduler {
	return &queueScheduler{
		queues: map[string]*taskQueue{},
		order:  []string{},
	}
}

func (s *queueScheduler) Len() int {
	return s.total
}

func (s *queueScheduler) Push(task *Task, weight int) {
	if task == nil {
		return
	}

	queue := normalizeQueueName(task.Queue)
	task.Queue = queue
	weight = normalizeQueueWeight(weight)

	queueState, ok := s.queues[queue]
	if !ok {
		queueState = &taskQueue{
			name:   queue,
			weight: weight,
			tasks:  newTaskHeap(),
		}
		s.queues[queue] = queueState
		s.order = append(s.order, queue)
		sort.Strings(s.order)
	} else if weight > 0 {
		queueState.weight = weight
	}

	queueState.tasks.Push(task)

	s.total++
}

func (s *queueScheduler) Pop() (*Task, bool) {
	if s.total == 0 || len(s.order) == 0 {
		return nil, false
	}

	attempts := 0
	for attempts < len(s.order) {
		if s.cursor >= len(s.order) {
			s.cursor = 0
		}

		queueState := s.queues[s.order[s.cursor]]
		if queueState == nil || queueState.tasks.Len() == 0 {
			if queueState != nil {
				queueState.remaining = 0
			}

			s.cursor++
			attempts++

			continue
		}

		if queueState.remaining <= 0 {
			queueState.remaining = normalizeQueueWeight(queueState.weight)
		}

		task, ok := queueState.tasks.Pop()
		if !ok {
			queueState.remaining = 0
			s.cursor++
			attempts++

			continue
		}

		s.total--

		queueState.remaining--
		if queueState.remaining <= 0 {
			s.cursor++
		}

		return task, true
	}

	return nil, false
}

func (s *queueScheduler) Remove(task *Task) (*Task, bool) {
	if task == nil || task.index < 0 {
		return nil, false
	}

	queue := normalizeQueueName(task.Queue)

	queueState, ok := s.queues[queue]
	if !ok || queueState.tasks.Len() == 0 {
		return nil, false
	}

	removed, ok := queueState.tasks.Remove(task.index)
	if !ok {
		return nil, false
	}

	s.total--

	if queueState.tasks.Len() == 0 {
		queueState.remaining = 0
	}

	return removed, true
}
