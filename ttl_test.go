package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/bytex"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func init() {
	err := os.RemoveAll(DefaultOptions().DirPath)
	if err != nil {
		panic(err.Error())
	}
}

func TestDB_TTL(t *testing.T) {
	options := DefaultOptions()
	options.SyncWrites = false
	db, err := NewCouloyDB(options)

	assert.NotNil(t, db)
	assert.Nil(t, err)

	defer destroyCouloyDB(db)

	err = db.PutWithExpiration(bytex.GetTestKey(0), bytex.RandomBytes(24), 3*time.Second)
	assert.Nil(t, err)

	err = db.PutWithExpiration(bytex.GetTestKey(1), bytex.RandomBytes(24), 1*time.Second)
	assert.Nil(t, err)

	time.Sleep(1 * time.Second)

	// after one second, key 000000001 should have expired and been deleted
	_, err = db.Get(bytex.GetTestKey(1))
	assert.NotNil(t, err)
	assert.Equal(t, public.ErrKeyNotFound, err)

	// but the key 000000000 can still be got
	value, err := db.Get(bytex.GetTestKey(0))
	assert.NotNil(t, value)
	assert.Nil(t, err)

	time.Sleep(2 * time.Second)

	// after two seconds, key 000000000 should have expired and been deleted
	value, err = db.Get(bytex.GetTestKey(0))
	assert.Nil(t, value)
	assert.NotNil(t, err)
}

func TestDB_TTL_Restart(t *testing.T) {
	options := DefaultOptions()
	options.SyncWrites = false
	db, err := NewCouloyDB(options)

	assert.NotNil(t, db)
	assert.Nil(t, err)

	err = db.PutWithExpiration(bytex.GetTestKey(0), bytex.RandomBytes(24), 2*time.Second)
	assert.Nil(t, err)

	value, err := db.Get(bytex.GetTestKey(0))
	assert.NotNil(t, value)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	time.Sleep(2000 * time.Millisecond)

	db, err = NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	defer destroyCouloyDB(db)

	// after restart, the previously set expiration time is still valid
	value, err = db.Get(bytex.GetTestKey(0))
	assert.Nil(t, value)
	assert.NotNil(t, err)
}

func TestDB_TTL_Persist(t *testing.T) {
	options := DefaultOptions()
	options.SyncWrites = false
	db, err := NewCouloyDB(options)

	assert.NotNil(t, db)
	assert.Nil(t, err)

	defer destroyCouloyDB(db)

	err = db.PutWithExpiration(bytex.GetTestKey(0), bytex.RandomBytes(24), 100*time.Millisecond)
	assert.Nil(t, err)

	db.Persist(bytex.GetTestKey(0))

	time.Sleep(100 * time.Millisecond)

	value, err := db.Get(bytex.GetTestKey(0))
	assert.Nil(t, err)
	assert.NotNil(t, value)
}

func TestDB_TTL_Reset(t *testing.T) {
	options := DefaultOptions()
	options.SyncWrites = false
	db, err := NewCouloyDB(options)

	assert.NotNil(t, db)
	assert.Nil(t, err)

	defer destroyCouloyDB(db)

	err = db.PutWithExpiration(bytex.GetTestKey(0), bytex.RandomBytes(24), 100*time.Millisecond)
	assert.Nil(t, err)

	err = db.Put(bytex.GetTestKey(0), bytex.RandomBytes(24))
	assert.Nil(t, err)

	// since the second put operation reset the expiration of key 000000000, it doesn't expire
	time.Sleep(100 * time.Millisecond)

	value, err := db.Get(bytex.GetTestKey(0))
	assert.Nil(t, err)
	assert.NotNil(t, value)
}
