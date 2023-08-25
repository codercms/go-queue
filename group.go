package queue

import "sync/atomic"

type TaskGroup[Result, ProcessArg any] struct {
	tasks []*Task[Result, ProcessArg]

	doneChan    chan struct{}
	resultsChan chan *TaskResult[Result]

	tasksLeft atomic.Int32
}

// Done returns channel similar to context.Done()
func (g *TaskGroup[Result, ProcessArg]) Done() chan struct{} {
	return g.doneChan
}

// Results returns the channel from which to read the results
func (g *TaskGroup[Result, ProcessArg]) Results() chan *TaskResult[Result] {
	return g.resultsChan
}

func (g *TaskGroup[Result, ProcessArg]) GetTasks() []*Task[Result, ProcessArg] {
	return g.tasks
}
