package worker

import "fmt"

// Result is a task result
type Result struct {
	Task   *Task       // the task that produced the result
	Result interface{} // the result of the task
	Error  error       // the error returned by the task
}

// String returns a string representation of the result
func (r *Result) String() string {
	return fmt.Sprintf("Task: %v, Result: %v, Error: %v", r.Task, r.Result, r.Error)
}
