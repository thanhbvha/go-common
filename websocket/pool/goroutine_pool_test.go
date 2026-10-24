package pool

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerContinuesAfterTaskPanic(t *testing.T) {
	workerPool := NewWorkerPool(1)
	t.Cleanup(func() { workerPool.Shutdown(time.Second) })

	panicked := make(chan struct{})
	if !workerPool.SubmitWithMetadata(TaskMetadata{Type: "panic-test"}, func() {
		close(panicked)
		panic("synthetic worker panic")
	}) {
		t.Fatal("failed to submit panicking task")
	}

	select {
	case <-panicked:
	case <-time.After(time.Second):
		t.Fatal("panicking task did not start")
	}

	var completed atomic.Bool
	done := make(chan struct{})
	if !workerPool.Submit(func() {
		completed.Store(true)
		close(done)
	}) {
		t.Fatal("failed to submit follow-up task")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not continue after task panic")
	}
	if !completed.Load() {
		t.Fatal("follow-up task did not complete")
	}
}
