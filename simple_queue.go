package queue

import (
	"errors"
	"github.com/panjf2000/ants/v2"
	"time"
)

// SimpleQueue is a plain wrapper around ants.Pool and doesn't offer any extra functionality
type SimpleQueue[TaskType any] struct {
	pool *ants.PoolWithFunc

	processFunc func(task TaskType)
}

func MakeSimpleQueue[TaskType any](
	workersCnt int,
	processFunc func(task TaskType),
	antsOptions ...ants.Option,
) (*SimpleQueue[TaskType], error) {
	if processFunc == nil {
		return nil, errors.New("processFunc is nil")
	}

	queue := &SimpleQueue[TaskType]{
		processFunc: processFunc,
	}

	pool, err := ants.NewPoolWithFunc(workersCnt, queue.process, antsOptions...)
	if err != nil {
		return nil, err
	}

	queue.pool = pool

	return queue, nil
}

func (q *SimpleQueue[TaskType]) GetFreeWorkersCount() int {
	return q.pool.Free()
}

func (q *SimpleQueue[TaskType]) GetNumTasksBlocked() int {
	return q.pool.Waiting()
}

func (q *SimpleQueue[TaskType]) GetWorkersCount() int {
	return q.pool.Cap()
}

func (q *SimpleQueue[TaskType]) SetWorkersCount(workers int) {
	if workers < 0 {
		workers = 1
	}

	q.pool.Tune(workers)
}

func (q *SimpleQueue[TaskType]) Stop() {
	q.pool.Release()
}

func (q *SimpleQueue[TaskType]) StopGraceful(timeout time.Duration) error {
	return q.pool.ReleaseTimeout(timeout)
}

func (q *SimpleQueue[TaskType]) Enqueue(task TaskType) error {
	return q.pool.Invoke(task)
}

func (q *SimpleQueue[TaskType]) EnqueueGroup(tasks ...TaskType) error {
	for _, task := range tasks {
		if err := q.pool.Invoke(task); err != nil {
			return err
		}
	}

	return nil
}

func (q *SimpleQueue[TaskType]) process(arg any) {
	task := arg.(TaskType)

	q.processFunc(task)
}
