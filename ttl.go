package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/public/ds"
	"sync"
	"sync/atomic"
	"time"
)

type ttl struct {
	mu       *sync.Mutex
	started  *atomic.Bool
	eventCh  chan struct{}
	timeHeap *ds.TimeHeap
	deleter  func(key string) error
}

func newTTL(deleter func(key string) error) *ttl {
	return &ttl{
		mu:       &sync.Mutex{},
		started:  &atomic.Bool{},
		eventCh:  make(chan struct{}),
		timeHeap: ds.NewTimeHeap(),
		deleter:  deleter,
	}
}

func (ttl *ttl) add(job *ds.Job) {
	ttl.timeHeap.Push(job)
	ttl.notify()
}

func (ttl *ttl) del(key string) {
	ttl.timeHeap.Remove(key)
	ttl.notify()
}

func (ttl *ttl) isExpired(key string) bool {
	job := ttl.timeHeap.Get(key)
	return job != nil && !job.Expiration.After(time.Now())
}

func (ttl *ttl) start() {
	ttl.started.Store(true)

	for {
		if !ttl.started.Load() {
			break
		}

		ttl.exec()
	}
}

func (ttl *ttl) stop() {
	ttl.started.Store(false)
	close(ttl.eventCh)
}

const MaxDuration time.Duration = 1<<63 - 1

func (ttl *ttl) exec() {
	now := time.Now()
	duration := MaxDuration

	job := ttl.timeHeap.Peek()

	if job != nil {
		if job.Expiration.After(now) {
			duration = job.Expiration.Sub(now)
		} else {
			duration = 0
		}
	}

	if duration > 0 {
		timer := time.NewTimer(duration)
		defer timer.Stop()

		select {
		case <-ttl.eventCh:
			return
		case <-timer.C:
		}
	}

	job = ttl.timeHeap.Pop()

	if job == nil {
		return
	}

	go ttl.deleter(job.Key)
}

func (ttl *ttl) notify() {
	if ttl.started.Load() {
		ttl.eventCh <- struct{}{}
	}
}
