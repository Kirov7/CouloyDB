package CouloyDB

import (
	"context"
	"github.com/Kirov7/CouloyDB/public/ds"
	"sync"
	"time"
)

type EventType byte

const (
	PutEvent EventType = iota
	DelEvent
)

type WatchEvent struct {
	Key       string
	Value     []byte
	EventType EventType
}

type WatcherManager struct {
	lock     *sync.RWMutex
	watchers map[string]map[*Watcher]struct{} // key to watchers
	queue    *ds.EventQueue
	closeCh  chan struct{}
}

func NewWatcherManager() *WatcherManager {
	return &WatcherManager{
		lock:     &sync.RWMutex{},
		watchers: make(map[string]map[*Watcher]struct{}),
		queue:    ds.NewEventQueue(),
		closeCh:  make(chan struct{}),
	}
}

func (wm *WatcherManager) closeWatcherListener(watcher *Watcher) {
	select {
	case <-watcher.ctx.Done():
		wm.lock.Lock()
		wm.UnWatch(watcher)
		wm.lock.Unlock()
	case <-wm.closeCh:
		return
	}
}

func (wm *WatcherManager) Watch(ctx context.Context, key string) <-chan *WatchEvent {

	watcher := &Watcher{
		key:      key,
		ctx:      ctx,
		respCh:   make(chan *WatchEvent, 128),
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

func (wm *WatcherManager) UnWatch(watcher *Watcher) {
	if !watcher.canceled {
		close(watcher.respCh)
		watcher.canceled = true
	}

	delete(wm.watchers[watcher.key], watcher)
	if len(wm.watchers[watcher.key]) == 0 {
		delete(wm.watchers, watcher.key)
	}
}

func (wm *WatcherManager) Watched(key string) bool {
	_, ok := wm.watchers[key]
	return ok
}

func (wm *WatcherManager) Notify(watchEvent *WatchEvent) {
	wm.queue.Write(watchEvent)
}

func (wm *WatcherManager) Start() {
	for {
		event, ok := wm.queue.Read().(*WatchEvent)

		if !ok {
			break
		}

		wm.lock.RLock()
		for watcher := range wm.watchers[event.Key] {
			if watcher.canceled {
				continue
			}

			watcher.sendResp(event)
		}
		wm.lock.RUnlock()
	}
}

func (wm *WatcherManager) Close() {
	wm.queue.Close()

	close(wm.closeCh)

	wm.lock.Lock()
	defer wm.lock.Unlock()

	for _, watchers := range wm.watchers {
		for watcher := range watchers {
			wm.UnWatch(watcher)
		}
	}
}

type Watcher struct {
	key      string
	ctx      context.Context
	respCh   chan *WatchEvent
	canceled bool
}

func (w *Watcher) sendResp(event *WatchEvent) {
	timeout := 100 * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case w.respCh <- event:
	case <-w.ctx.Done():
	case <-timer.C:
	}
}
