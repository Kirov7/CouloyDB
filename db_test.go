package CouloyDB

import (
	"fmt"
	"os"
	"testing"

	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/bytex"
	"github.com/Kirov7/CouloyDB/public/utils/wait"
	"github.com/stretchr/testify/assert"
)

func destroyCouloyDB(db *DB) {
	err := db.Close()
	if err != nil {
		panic(err.Error())
	}
	err = os.RemoveAll(db.options.DirPath)
	if err != nil {
		panic(err.Error())
	}
}

func TestNewCouloyDB(t *testing.T) {
	options := DefaultOptions()
	couloyDB, err := NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)
}

func TestDB_Put(t *testing.T) {
	options := DefaultOptions()
	options.DataFileSize = 8 * 1024 * 1024
	options.SyncWrites = false
	couloyDB, err := NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	// Normally put key 1 and a random value to db
	err = couloyDB.Put(bytex.GetTestKey(1), bytex.RandomBytes(6))
	assert.Nil(t, err)
	value, err := couloyDB.Get(bytex.GetTestKey(1))
	assert.NotNil(t, value)
	assert.Nil(t, err)

	// Test repeatedly key
	err = couloyDB.Put(bytex.GetTestKey(1), bytex.RandomBytes(6))
	assert.Nil(t, err)
	value, err = couloyDB.Get(bytex.GetTestKey(1))
	assert.NotNil(t, value)
	assert.Nil(t, err)

	// Test empty key
	err = couloyDB.Put([]byte{}, bytex.RandomBytes(6))
	assert.Equal(t, err, public.ErrKeyIsEmpty)

	// Test empty value
	err = couloyDB.Put(bytex.GetTestKey(2), []byte{})
	assert.Nil(t, err)
	value, err = couloyDB.Get(bytex.GetTestKey(2))
	assert.Nil(t, err)
	assert.Len(t, value, 0)
}

func TestDB_Get(t *testing.T) {
	options := DefaultOptions()
	options.DataFileSize = 8 * 1024 * 1024
	options.SyncWrites = false
	couloyDB, err := NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	err = couloyDB.Put(bytex.GetTestKey(1), bytex.GetTestKey(1))
	assert.Nil(t, err)

	// Test get normally
	value, err := couloyDB.Get(bytex.GetTestKey(1))
	assert.NotNil(t, value)
	assert.Nil(t, err)
	assert.Equal(t, value, bytex.GetTestKey(1))

	// Test get when key is empty
	value, err = couloyDB.Get([]byte{})
	assert.Nil(t, value)
	assert.NotNil(t, err)
	assert.Equal(t, public.ErrKeyIsEmpty, err)

	// Test get when the same key changes
	err = couloyDB.Put(bytex.GetTestKey(2), bytex.RandomBytes(64))
	v1, err := couloyDB.Get(bytex.GetTestKey(2))
	assert.Nil(t, err)
	err = couloyDB.Put(bytex.GetTestKey(2), bytex.RandomBytes(64))
	v2, err := couloyDB.Get(bytex.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotEqual(t, v1, v2)

	// Test get after delete key
	err = couloyDB.Del(bytex.GetTestKey(2))
	assert.Nil(t, err)
	value, err = couloyDB.Get(bytex.GetTestKey(2))
	assert.Nil(t, value)
	assert.NotNil(t, err)
	assert.Equal(t, public.ErrKeyNotFound, err)
}

func TestDB_Put_Get_Concurrency(t *testing.T) {
	options := DefaultOptions()
	options.DataFileSize = 8 * 1024 * 1024
	options.SyncWrites = false
	couloyDB, err := NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	w := wait.NewWait()

	workerNum := 200

	// Test 200 put workers put kv parallel
	w.Add(workerNum)
	for id := 0; id < workerNum; id++ {
		go func(id int) {
			defer w.Done()
			err := couloyDB.Put(bytex.GetTestKey(id), bytex.GetTestKey(id))
			assert.Nil(t, err)
		}(id)
	}

	w.Wait()

	// Test whether the 200 put workers just now are successful
	w.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func(id int) {
			defer w.Done()
			value, err := couloyDB.Get(bytex.GetTestKey(id))
			assert.Nil(t, err)
			assert.NotNil(t, value)
		}(i)
	}

	w.Wait()

	// Test create new data file, and read from the new data file and old data files
	workerNum = 10000

	fill := make([]byte, 1024)

	// This will make the active data file too large and create a new data file instead
	w.Add(workerNum)
	for id := 0; id < workerNum; id++ {
		go func(id int) {
			defer w.Done()
			key := bytex.GetTestKey(id)
			err := couloyDB.Put(key, append(key, fill...))
			assert.Nil(t, err)
		}(id)
	}

	w.Wait()

	// Because there are multiple data files now, it must be read from multiple data files
	w.Add(workerNum)
	for id := 0; id < workerNum; id++ {
		go func(id int) {
			defer w.Done()
			key := bytex.GetTestKey(id)
			value, err := couloyDB.Get(key)
			assert.Nil(t, err)
			assert.Equal(t, value, append(key, fill...))
		}(id)
	}

	w.Wait()

	// must have old file
	assert.NotEqual(t, len(couloyDB.oldFile), 0)
}

func TestDB_Del(t *testing.T) {
	options := DefaultOptions()
	options.DataFileSize = 8 * 1024 * 1024
	options.SyncWrites = false
	couloyDB, err := NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	err = couloyDB.Put(bytex.GetTestKey(1), bytex.GetTestKey(1))
	assert.Nil(t, err)

	// Test del normally
	err = couloyDB.Del(bytex.GetTestKey(1))
	assert.Nil(t, err)
	value, err := couloyDB.Get(bytex.GetTestKey(2))
	assert.Nil(t, value)
	assert.NotNil(t, err)
	assert.Equal(t, public.ErrKeyNotFound, err)

	// Test del an empty key
	err = couloyDB.Del([]byte{})
	assert.NotNil(t, err)
	assert.Equal(t, err, public.ErrKeyIsEmpty)

	// Test del a key that is not exist
	err = couloyDB.Del([]byte("not exist!"))
	assert.Nil(t, err)
}

func TestDB_Reboot(t *testing.T) {
	options := DefaultOptions()
	options.DataFileSize = 8 * 1024 * 1024
	options.SyncWrites = false
	couloyDB, err := NewCouloyDB(options)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	w := wait.NewWait()
	w.Wait()

	workerNum := 10000

	fill := make([]byte, 1024)

	w.Add(workerNum)
	for id := 0; id < workerNum; id++ {
		go func(id int) {
			defer w.Done()
			key := bytex.GetTestKey(id)
			err := couloyDB.Put(key, append(key, fill...))
			assert.Nil(t, err)
		}(id)
	}

	w.Wait()

	// Reboot couloydb
	err = couloyDB.Close()
	assert.Nil(t, err)
	couloyDB, err = NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	// Test whether the data can still be read from the previous file after the db is restarted
	w.Add(workerNum)
	for id := 0; id < workerNum; id++ {
		go func(id int) {
			defer w.Done()
			key := bytex.GetTestKey(id)
			value, err := couloyDB.Get(key)
			assert.Nil(t, err)
			assert.Equal(t, value, append(key, fill...))
		}(id)
	}
	w.Wait()
}

func TestDB_Fold(t *testing.T) {
	options := DefaultOptions()
	options.SetSyncWrites(false)
	couloyDB, err := NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	// put some key-value
	var n = 3
	for i := 0; i < n; i++ {
		err = couloyDB.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
		assert.Nil(t, err)
	}

	// define a map that collects all key-value pairs
	results := make(map[string]string)

	err = couloyDB.Fold(func(key []byte, value []byte) bool {
		results[string(key)] = string(value)
		return true
	})
	assert.Nil(t, err)

	// check the result
	assert.Equal(t, n, len(results))

	// test the early termination feature of the Fold func
	count := 0
	err = couloyDB.Fold(func(key []byte, value []byte) bool {
		count++
		if count == 2 {
			return false
		}
		return true
	})
	assert.Nil(t, err)
	assert.Equal(t, count, 2)
}

func TestDB_SADD(t *testing.T) {
	options := DefaultOptions()
	options.DataFileSize = 8 * 1024 * 1024
	options.SyncWrites = false
	couloyDB, err := NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	// Normally put key 1 and a random value to db
	err = couloyDB.SADD(bytex.GetTestKey(1), bytex.RandomBytes(6))
	assert.Nil(t, err)
	count, err := couloyDB.SCARD(bytex.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, count, 1)
	values, err := couloyDB.SMEMBERS(bytex.GetTestKey(1))
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.NotNil(t, values)
	assert.Nil(t, err)

	// Test repeatedly key
	err = couloyDB.SADD(bytex.GetTestKey(1), bytex.RandomBytes(6))
	assert.Nil(t, err)
	values, err = couloyDB.SMEMBERS(bytex.GetTestKey(1))
	assert.NotNil(t, values)
	assert.Nil(t, err)

	// Test empty key
	err = couloyDB.SADD([]byte{}, bytex.RandomBytes(6))
	assert.Equal(t, err, public.ErrKeyIsEmpty)

	// Test empty value
	err = couloyDB.SADD(bytex.GetTestKey(2), []byte{})
	assert.Error(t, err, public.ErrKeyIsEmpty)
}
