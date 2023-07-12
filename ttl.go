package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/public/ds"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type ttl struct {
	mu       *sync.RWMutex
	started  *atomic.Bool
	eventCh  chan struct{}
	timeHeap *ds.TimeHeap
	deleter  func(key string) error
}

func newTTL(deleter func(key string) error) *ttl {
	return &ttl{
		mu:       &sync.RWMutex{},
		started:  &atomic.Bool{},
		eventCh:  make(chan struct{}),
		timeHeap: ds.NewTimeHeap(),
		deleter:  deleter,
	}
}

func (ttl *ttl) add(job *ds.Job) {
	ttl.mu.Lock()
	ttl.timeHeap.Push(job)
	ttl.mu.Unlock()
	ttl.notify()
}

func (ttl *ttl) del(key string) {
	ttl.mu.Lock()
	ttl.timeHeap.Remove(key)
	ttl.mu.Unlock()
	ttl.notify()
}

func (ttl *ttl) isExpired(key string) bool {
	ttl.mu.RLock()
	defer ttl.mu.RUnlock()
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
	ttl.mu.Lock()
	close(ttl.eventCh)
	ttl.mu.Unlock()
}

const MaxDuration time.Duration = 1<<63 - 1

func (ttl *ttl) exec() {
	now := time.Now()
	duration := MaxDuration

	ttl.mu.RLock()
	job := ttl.timeHeap.Peek()
	ttl.mu.RUnlock()

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

	ttl.mu.Lock()
	job = ttl.timeHeap.Pop()
	ttl.mu.Unlock()

	if job == nil {
		return
	}

	go func() {
		if err := ttl.deleter(job.Key); err != nil {
			log.Printf("there is a error occured by deleter: %v", err.Error())
		}
	}()
}

func (ttl *ttl) notify() {
	if ttl.started.Load() {
		ttl.eventCh <- struct{}{}
	}
}
