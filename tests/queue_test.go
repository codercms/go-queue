package tests

import (
	"cmp"
	"github.com/codercms/go-queue"
	"github.com/panjf2000/ants/v2"
	"log"
	"slices"
	"time"

	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue_Enqueue(t *testing.T) {
	_queue, err := queue.MakeQueue(1, func(taskId int64, arg int) (int, error) {
		return arg + arg, nil
	})
	assert.NoError(t, err)

	task, err := _queue.Enqueue(2)
	assert.NoError(t, err)

	select {
	case res := <-task.Result():
		assert.NoError(t, res.Error)
		assert.Equal(t, 4, res.Result)
	case <-time.After(time.Second):
		assert.Fail(t, "task done await failed")
		return
	}
}

func TestQueue_EnqueueWait(t *testing.T) {
	_queue, err := queue.MakeQueue(1, func(taskId int64, arg int) (int, error) {
		return arg + arg, nil
	})
	assert.NoError(t, err)

	task, err := _queue.Enqueue(2)
	assert.NoError(t, err)

	select {
	case <-task.Done():
		log.Printf("task done received")
	case <-time.After(time.Second):
		assert.Fail(t, "task done await failed")
		return
	}

	select {
	case res := <-task.Result():
		assert.NoError(t, res.Error)
		assert.Equal(t, 4, res.Result)
	case <-time.After(time.Second):
		assert.Fail(t, "task done await failed")
		return
	}
}

func TestQueue_EnqueueGroup(t *testing.T) {
	_queue, err := queue.MakeQueue(4, func(taskId int64, arg int) (int, error) {
		return arg + arg, nil
	}, ants.WithPreAlloc(true))

	assert.NoError(t, err)

	args := []int{2, 4, 6, 8, 10, 12}

	grp, err := _queue.EnqueueGroup(args...)
	assert.NoError(t, err)

	assert.Len(t, grp.GetTasks(), len(args))

	results := make([]*queue.TaskResult[int, int], 0, len(args))

	timeoutCh := time.After(time.Second)

	var done bool
	for !done {
		select {
		case res, ok := <-grp.Results():
			if !ok {
				done = true
				break
			}

			if !assert.NoError(t, res.Error) {
				return
			}

			results = append(results, res)
		case <-timeoutCh:
			assert.Fail(t, "task done await failed")
			return
		}
	}

	assert.Len(t, results, len(args))

	slices.SortStableFunc(results, func(a, b *queue.TaskResult[int, int]) int {
		return cmp.Compare(a.Task.GetID(), b.Task.GetID())
	})

	for idx, arg := range args {
		assert.Equal(t, arg+arg, results[idx].Result, "wrong value for arg no. ", idx)
	}
}
