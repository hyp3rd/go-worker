package worker

import "time"

const (
	defaultDurableLease        = 30 * time.Second
	defaultDurablePollInterval = 200 * time.Millisecond
)

// TaskManagerOption configures a TaskManager.
type TaskManagerOption func(*taskManagerConfig)

type taskManagerConfig struct {
	maxWorkers     int
	maxTasks       int
	tasksPerSecond float64
	timeout        time.Duration
	retryDelay     time.Duration
	maxRetries     int

	durableBackend      DurableBackend
	durableHandlers     map[string]DurableHandlerSpec
	durableCodec        DurableCodec
	durableLease        time.Duration
	durablePollInterval time.Duration
	durableBatchSize    int
}

func defaultTaskManagerConfig() taskManagerConfig {
	return taskManagerConfig{
		maxWorkers:          0,
		maxTasks:            DefaultMaxTasks,
		tasksPerSecond:      DefaultTasksPerSecond,
		timeout:             DefaultTimeout,
		retryDelay:          DefaultRetryDelay,
		maxRetries:          DefaultMaxRetries,
		durableLease:        defaultDurableLease,
		durablePollInterval: defaultDurablePollInterval,
		durableBatchSize:    1,
		durableCodec:        ProtoDurableCodec{},
	}
}

// WithMaxWorkers sets the maximum number of workers.
func WithMaxWorkers(n int) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.maxWorkers = n
	}
}

// WithMaxTasks sets the maximum number of tasks in the queue.
func WithMaxTasks(n int) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.maxTasks = n
	}
}

// WithTasksPerSecond sets the maximum number of tasks to start per second.
func WithTasksPerSecond(n float64) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.tasksPerSecond = n
	}
}

// WithTimeout sets the task execution timeout.
func WithTimeout(timeout time.Duration) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.timeout = timeout
	}
}

// WithRetryDelay sets the delay between task retries.
func WithRetryDelay(delay time.Duration) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.retryDelay = delay
	}
}

// WithMaxRetries sets the maximum number of retries for a task.
func WithMaxRetries(n int) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.maxRetries = n
	}
}

// WithDurableBackend sets the durable backend for the TaskManager.
func WithDurableBackend(backend DurableBackend) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.durableBackend = backend
	}
}

// WithDurableHandlers sets the durable handlers for the TaskManager.
func WithDurableHandlers(handlers map[string]DurableHandlerSpec) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.durableHandlers = handlers
	}
}

// WithDurableCodec sets the durable codec for the TaskManager.
func WithDurableCodec(codec DurableCodec) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		if codec != nil {
			cfg.durableCodec = codec
		}
	}
}

// WithDurableLease sets the durable task lease duration.
func WithDurableLease(lease time.Duration) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.durableLease = lease
	}
}

// WithDurablePollInterval sets the durable task polling interval.
func WithDurablePollInterval(interval time.Duration) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.durablePollInterval = interval
	}
}

// WithDurableBatchSize sets the durable task batch size.
func WithDurableBatchSize(size int) TaskManagerOption {
	return func(cfg *taskManagerConfig) {
		cfg.durableBatchSize = size
	}
}
