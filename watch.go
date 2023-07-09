package CouloyDB

import (
	"context"
	"github.com/Kirov7/CouloyDB/public/ds"
	"sync"
	"time"
)

type eventType byte

const (
	PutEvent eventType = iota
	DelEvent
)

type watchEvent struct {
	key       string
	value     []byte
	eventType eventType
}

type watcherManager struct {
	lock     *sync.RWMutex
	watchers map[string]map[*Watcher]struct{} // key to watchers
	queue    *ds.EventQueue
	closeCh  chan struct{}
}

func newWatcherManager() *watcherManager {
	return &watcherManager{
		lock:     &sync.RWMutex{},
		watchers: make(map[string]map[*Watcher]struct{}),
		queue:    ds.NewEventQueue(),
		closeCh:  make(chan struct{}),
	}
}

func (wm *watcherManager) closeWatcherListener(watcher *Watcher) {
	select {
	case <-watcher.ctx.Done():
		wm.lock.Lock()
		wm.unWatch(watcher)
		wm.lock.Unlock()
	case <-wm.closeCh:
		return
	}
}

func (wm *watcherManager) watch(ctx context.Context, key string) <-chan *watchEvent {

	watcher := &Watcher{
		key:      key,
		ctx:      ctx,
		respCh:   make(chan *watchEvent, 128),
		canceled: false,
	}

	wm.lock.Lock()
	defer wm.lock.Unlock()

	_, ok := wm.watchers[key]
	if !ok {
		wm.watchers[key] = make(map[*Watcher]struct{})
	}

	wm.watchers[key][watcher] = struct{}{}

	go wm.closeWatcherListener(watcher)

	return watcher.respCh
}

func (wm *watcherManager) unWatch(watcher *Watcher) {
	if !watcher.canceled {
		close(watcher.respCh)
		watcher.canceled = true
	}

	delete(wm.watchers[watcher.key], watcher)
	if len(wm.watchers[watcher.key]) == 0 {
		delete(wm.watchers, watcher.key)
	}
}

func (wm *watcherManager) watched(key string) bool {
	_, ok := wm.watchers[key]
	return ok
}

func (wm *watcherManager) notify(watchEvent *watchEvent) {
	wm.queue.Write(watchEvent)
}

func (wm *watcherManager) start() {
	for {
		event, ok := wm.queue.Read().(*watchEvent)

		if !ok {
			break
		}

		wm.lock.RLock()
		for watcher := range wm.watchers[event.key] {
			watcher := watcher
			if watcher.canceled {
				continue
			}

			go watcher.sendResp(event)
		}
		wm.lock.RUnlock()
	}
}

func (wm *watcherManager) stop() {
	wm.queue.Close()

	close(wm.closeCh)

	wm.lock.Lock()
	defer wm.lock.Unlock()

	for _, watchers := range wm.watchers {
		for watcher := range watchers {
			wm.unWatch(watcher)
		}
	}
}

type Watcher struct {
	key      string
	ctx      context.Context
	respCh   chan *watchEvent
	canceled bool
}

func (w *Watcher) sendResp(event *watchEvent) {
	timeout := 100 * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case w.respCh <- event:
	case <-w.ctx.Done():
	case <-timer.C:
	}
}
