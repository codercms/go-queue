package queue

type Task[Result, ProcessArg any] struct {
	id int64

	arg ProcessArg

	grp *TaskGroup[Result, ProcessArg]

	doneChan   chan struct{}
	resultChan chan *TaskResult[Result]
}

func (t *Task[Result, ProcessArg]) GetID() int64 {
	return t.id
}

func (t *Task[Result, ProcessArg]) GetArg() ProcessArg {
	return t.arg
}

// Done returns channel similar to context.Done()
func (t *Task[Result, ProcessArg]) Done() chan struct{} {
	return t.doneChan
}

// Result returns the channel from which to read the result
func (t *Task[Result, ProcessArg]) Result() chan *TaskResult[Result] {
	return t.resultChan
}
