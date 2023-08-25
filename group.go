package queue

import "sync/atomic"

type TaskGroup[ResultType, TaskArg any] struct {
	tasks []*Task[ResultType, TaskArg]

	doneChan    chan struct{}
	resultsChan chan *TaskResult[ResultType, TaskArg]

	tasksLeft atomic.Int32
}

// Done returns channel similar to context.Done()
func (g *TaskGroup[ResultType, TaskArg]) Done() chan struct{} {
	return g.doneChan
}

// Results returns the channel from which to read the results
func (g *TaskGroup[ResultType, TaskArg]) Results() chan *TaskResult[ResultType, TaskArg] {
	return g.resultsChan
}

func (g *TaskGroup[ResultType, TaskArg]) GetTasks() []*Task[ResultType, TaskArg] {
	return g.tasks
}
