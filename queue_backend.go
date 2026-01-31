package worker

// QueueConfigurableBackend allows TaskManager to propagate queue configuration to backends.
type QueueConfigurableBackend interface {
	ConfigureQueues(defaultQueue string, weights map[string]int)
}
