package CouloyDB

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDB_Watch(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	defer destroyCouloyDB(db)

	key := "CouloyDB"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	watchCh := db.Watch(ctx, key)

	go func() {
		_ = db.Put([]byte(key), []byte("value1"))
		_ = db.Put([]byte(key), []byte("value2"))
		_ = db.Del([]byte(key))
	}()

	expectedEvents := []*watchEvent{
		{key: key, value: []byte("value1"), eventType: PutEvent},
		{key: key, value: []byte("value2"), eventType: PutEvent},
		{key: key, value: nil, eventType: DelEvent},
	}

	for _, expectedEvent := range expectedEvents {
		select {
		case <-ctx.Done():
			assert.Fail(t, "Context canceled before receiving all events")
			return
		case event, ok := <-watchCh:
			assert.True(t, ok)
			assert.Equal(t, expectedEvent, event)
		}
	}
}

func TestDB_Watch_Cancel(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	defer destroyCouloyDB(db)

	key := "key"
	ctx, cancel := context.WithCancel(context.Background())
	watchCh := db.wm.watch(ctx, key)

	go func() {
		cancel()
	}()

	select {
	case <-ctx.Done():
		break
	case _, _ = <-watchCh:
		assert.Fail(t, "Context should be canceled before receiving all events")
	}
}
