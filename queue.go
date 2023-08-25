package queue

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
)

type (
	Queue[Result any, ProcessArg any] struct {
		pool *ants.PoolWithFunc

		lastId atomic.Int64

		processFunc func(taskId int64, arg ProcessArg) (Result, error)
	}

	TaskResult[Result any] struct {
		TaskID int64
		Result Result
		Error  error
	}
)

func MakeQueue[Result any, ProcessArg any](
	workersCnt int,
	processFunc func(taskId int64, arg ProcessArg) (Result, error),
	antsOptions ...ants.Option,
) (*Queue[Result, ProcessArg], error) {
	if processFunc == nil {
		return nil, errors.New("processFunc is nil")
	}

	queue := &Queue[Result, ProcessArg]{
		processFunc: processFunc,
	}

	pool, err := ants.NewPoolWithFunc(workersCnt, queue.process, antsOptions...)
	if err != nil {
		return nil, err
	}

	queue.pool = pool

	return queue, nil
}

func (q *Queue[Result, ProcessArg]) GetFreeWorkersCount() int {
	return q.pool.Free()
}

func (q *Queue[Result, ProcessArg]) GetNumTasksBlocked() int {
	return q.pool.Waiting()
}

func (q *Queue[Result, ProcessArg]) GetWorkersCount() int {
	return q.pool.Cap()
}

func (q *Queue[Result, ProcessArg]) SetWorkersCount(workers int) {
	if workers < 0 {
		workers = 1
	}

	q.pool.Tune(workers)
}

func (q *Queue[Result, ProcessArg]) Stop() {
	q.pool.Release()
}

func (q *Queue[Result, ProcessArg]) StopGraceful(timeout time.Duration) error {
	return q.pool.ReleaseTimeout(timeout)
}

// Enqueue single task
func (q *Queue[Result, ProcessArg]) Enqueue(arg ProcessArg) (*Task[Result, ProcessArg], error) {
	return q.enqueueTask(arg, nil)
}

func (q *Queue[Result, ProcessArg]) enqueueTask(
	arg ProcessArg,
	grp *TaskGroup[Result, ProcessArg],
) (*Task[Result, ProcessArg], error) {
	task := Task[Result, ProcessArg]{
		id:         q.lastId.Add(1),
		arg:        arg,
		grp:        grp,
		doneChan:   make(chan struct{}),
		resultChan: make(chan *TaskResult[Result], 1),
	}

	if err := q.pool.Invoke(&task); err != nil {
		return nil, err
	}

	return &task, nil
}

// EnqueueGroup of tasks
func (q *Queue[Result, ProcessArg]) EnqueueGroup(args ...ProcessArg) (*TaskGroup[Result, ProcessArg], error) {
	grp := TaskGroup[Result, ProcessArg]{
		tasks:       make([]*Task[Result, ProcessArg], 0, len(args)),
		resultsChan: make(chan *TaskResult[Result], len(args)),
		doneChan:    make(chan struct{}),
	}

	leftCnt := len(args)

	for idx, arg := range args {
		grp.tasksLeft.Add(1)
		task, err := q.enqueueTask(arg, &grp)
		if err != nil {
			grp.tasksLeft.Add(-1)
			return &grp, fmt.Errorf("unable to enqueue task group, task %d enqueue failed: %w", idx, err)
		}

		leftCnt--
		grp.tasks = append(grp.tasks, task)
	}

	return &grp, nil
}

func (q *Queue[Result, ProcessArg]) process(arg any) {
	task := arg.(*Task[Result, ProcessArg])

	res, err := q.processFunc(task.id, task.arg)

	result := TaskResult[Result]{
		TaskID: task.id,
		Result: res,
		Error:  err,
	}

	task.resultChan <- &result
	close(task.doneChan)

	if task.grp != nil {
		task.grp.resultsChan <- &result

		// Close done chan when all tasks completed
		if task.grp.tasksLeft.Add(-1) <= 0 {
			close(task.grp.resultsChan)
			close(task.grp.doneChan)
		}
	}
}
