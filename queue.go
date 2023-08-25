package queue

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
)

type (
	Queue[ResultType any, TaskArg any] struct {
		pool *ants.PoolWithFunc

		lastId atomic.Int64

		processFunc func(taskId int64, arg TaskArg) (ResultType, error)
	}

	TaskResult[ResultType, TaskArg any] struct {
		Task   *Task[ResultType, TaskArg]
		Result ResultType
		Error  error
	}
)

func MakeQueue[ResultType any, TaskArg any](
	workersCnt int,
	processFunc func(taskId int64, arg TaskArg) (ResultType, error),
	antsOptions ...ants.Option,
) (*Queue[ResultType, TaskArg], error) {
	if processFunc == nil {
		return nil, errors.New("processFunc is nil")
	}

	queue := &Queue[ResultType, TaskArg]{
		processFunc: processFunc,
	}

	pool, err := ants.NewPoolWithFunc(workersCnt, queue.process, antsOptions...)
	if err != nil {
		return nil, err
	}

	queue.pool = pool

	return queue, nil
}

func (q *Queue[ResultType, TaskArg]) GetFreeWorkersCount() int {
	return q.pool.Free()
}

func (q *Queue[ResultType, TaskArg]) GetNumTasksBlocked() int {
	return q.pool.Waiting()
}

func (q *Queue[ResultType, TaskArg]) GetWorkersCount() int {
	return q.pool.Cap()
}

func (q *Queue[ResultType, TaskArg]) SetWorkersCount(workers int) {
	if workers < 0 {
		workers = 1
	}

	q.pool.Tune(workers)
}

func (q *Queue[ResultType, TaskArg]) Stop() {
	q.pool.Release()
}

func (q *Queue[ResultType, TaskArg]) StopGraceful(timeout time.Duration) error {
	return q.pool.ReleaseTimeout(timeout)
}

// Enqueue single task
func (q *Queue[ResultType, TaskArg]) Enqueue(arg TaskArg) (*Task[ResultType, TaskArg], error) {
	return q.enqueueTask(arg, nil)
}

func (q *Queue[ResultType, TaskArg]) enqueueTask(
	arg TaskArg,
	grp *TaskGroup[ResultType, TaskArg],
) (*Task[ResultType, TaskArg], error) {
	task := Task[ResultType, TaskArg]{
		id:         q.lastId.Add(1),
		arg:        arg,
		grp:        grp,
		doneChan:   make(chan struct{}),
		resultChan: make(chan *TaskResult[ResultType, TaskArg], 1),
	}

	if err := q.pool.Invoke(&task); err != nil {
		return nil, err
	}

	return &task, nil
}

// EnqueueGroup of tasks
func (q *Queue[ResultType, TaskArg]) EnqueueGroup(args ...TaskArg) (*TaskGroup[ResultType, TaskArg], error) {
	grp := TaskGroup[ResultType, TaskArg]{
		tasks:       make([]*Task[ResultType, TaskArg], 0, len(args)),
		resultsChan: make(chan *TaskResult[ResultType, TaskArg], len(args)),
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

func (q *Queue[ResultType, TaskArg]) process(arg any) {
	task := arg.(*Task[ResultType, TaskArg])

	res, err := q.processFunc(task.id, task.arg)

	result := TaskResult[ResultType, TaskArg]{
		Task:   task,
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
