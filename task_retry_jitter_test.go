package worker

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

const retryJitterTestDelay = time.Second

func TestRetryJitterDelayWithinBounds(t *testing.T) {
	t.Parallel()

	task := &Task{
		ID:         uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		RetryDelay: retryJitterTestDelay,
		Retries:    1,
	}

	task.initRetryState(retryJitterTestDelay, 1)

	delay, _, ok := task.nextRetryDelay()
	if !ok {
		t.Fatal("expected retry delay to be available")
	}

	span := retryJitterTestDelay / retryJitterDivisor
	lower := retryJitterTestDelay - span/2
	upper := retryJitterTestDelay + span/2

	if delay < lower || delay > upper {
		t.Fatalf("expected retry delay within [%v, %v], got %v", lower, upper, delay)
	}
}
