package queue

type Task[ResultType, TaskArg any] struct {
	id int64

	arg TaskArg

	grp *TaskGroup[ResultType, TaskArg]

	doneChan   chan struct{}
	resultChan chan *TaskResult[ResultType, TaskArg]
}

func (t *Task[ResultType, TaskArg]) GetID() int64 {
	return t.id
}

func (t *Task[ResultType, TaskArg]) GetArg() TaskArg {
	return t.arg
}

// Done returns channel similar to context.Done()
func (t *Task[ResultType, TaskArg]) Done() chan struct{} {
	return t.doneChan
}

// Result returns the channel from which to read the result
func (t *Task[ResultType, TaskArg]) Result() chan *TaskResult[ResultType, TaskArg] {
	return t.resultChan
}
