package ds

import (
	"sync"
)

type EventQueue struct {
	queue  []any
	cond   *sync.Cond
	closed bool
}

func NewEventQueue() *EventQueue {
	return &EventQueue{
		queue:  make([]any, 0),
		cond:   sync.NewCond(&sync.Mutex{}),
		closed: false,
	}
}

func (q *EventQueue) Read() any {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for len(q.queue) == 0 && !q.closed {
		q.cond.Wait()
	}

	if len(q.queue) == 0 {
		return nil
	}

	event := q.queue[0]
	q.queue[0] = nil
	q.queue = q.queue[1:]

	return event
}

func (q *EventQueue) Write(item any) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.closed {
		return
	}

	q.queue = append(q.queue, item)
	q.cond.Signal()
}

func (q *EventQueue) Close() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.closed {
		return
	}

	q.closed = true
	q.cond.Broadcast()
}
