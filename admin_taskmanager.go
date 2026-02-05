package worker

import "context"

func (tm *TaskManager) adminBackend() (AdminBackend, error) {
	if tm == nil {
		return nil, ErrAdminBackendUnavailable
	}

	if tm.durableBackend == nil {
		return nil, ErrAdminBackendUnavailable
	}

	backend, ok := tm.durableBackend.(AdminBackend)
	if !ok {
		return nil, ErrAdminUnsupported
	}

	return backend, nil
}

// AdminOverview returns an admin overview snapshot.
func (tm *TaskManager) AdminOverview(ctx context.Context) (AdminOverview, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return AdminOverview{}, err
	}

	overview, err := backend.AdminOverview(ctx)
	if err != nil {
		return AdminOverview{}, err
	}

	if overview.ActiveWorkers < 0 {
		overview.ActiveWorkers = tm.GetActiveTasks()
	}

	return overview, nil
}

// AdminQueues lists queue summaries.
func (tm *TaskManager) AdminQueues(ctx context.Context) ([]AdminQueueSummary, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return nil, err
	}

	return backend.AdminQueues(ctx)
}

// AdminDLQ lists DLQ entries.
func (tm *TaskManager) AdminDLQ(ctx context.Context, limit int) ([]AdminDLQEntry, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return nil, err
	}

	return backend.AdminDLQ(ctx, limit)
}

// AdminPause pauses durable dequeue.
func (tm *TaskManager) AdminPause(ctx context.Context) error {
	backend, err := tm.adminBackend()
	if err != nil {
		return err
	}

	return backend.AdminPause(ctx)
}

// AdminResume resumes durable dequeue.
func (tm *TaskManager) AdminResume(ctx context.Context) error {
	backend, err := tm.adminBackend()
	if err != nil {
		return err
	}

	return backend.AdminResume(ctx)
}

// AdminReplayDLQ replays DLQ entries.
func (tm *TaskManager) AdminReplayDLQ(ctx context.Context, limit int) (int, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return 0, err
	}

	return backend.AdminReplayDLQ(ctx, limit)
}
