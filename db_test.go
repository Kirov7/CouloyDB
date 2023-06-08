package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/bytes"
	"github.com/Kirov7/CouloyDB/public/utils/wait"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
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
	couloyDB, err := NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	// Normally put key 1 and a random value to db
	err = couloyDB.Put(bytes.IntToBytes(1), bytes.RandomBytes(6))
	assert.Nil(t, err)
	value, err := couloyDB.Get(bytes.IntToBytes(1))
	assert.NotNil(t, value)
	assert.Nil(t, err)

	// Test repeatedly key
	err = couloyDB.Put(bytes.IntToBytes(1), bytes.RandomBytes(6))
	assert.Nil(t, err)
	value, err = couloyDB.Get(bytes.IntToBytes(1))
	assert.NotNil(t, value)
	assert.Nil(t, err)

	// Test empty key
	err = couloyDB.Put([]byte{}, bytes.RandomBytes(6))
	assert.Equal(t, err, public.ErrKeyIsEmpty)

	// Test empty value
	err = couloyDB.Put(bytes.IntToBytes(2), []byte{})
	assert.Nil(t, err)
	value, err = couloyDB.Get(bytes.IntToBytes(2))
	assert.Nil(t, err)
	assert.Len(t, value, 0)
}

func TestDB_Get(t *testing.T) {
	options := DefaultOptions()
	options.DataFileSize = 8 * 1024 * 1024
	couloyDB, err := NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	err = couloyDB.Put(bytes.IntToBytes(1), bytes.IntToBytes(1))
	assert.Nil(t, err)

	// Test get normally
	value, err := couloyDB.Get(bytes.IntToBytes(1))
	assert.NotNil(t, value)
	assert.Nil(t, err)
	assert.Equal(t, value, bytes.IntToBytes(1))

	// Test get when key is empty
	value, err = couloyDB.Get([]byte{})
	assert.Nil(t, value)
	assert.NotNil(t, err)
	assert.Equal(t, public.ErrKeyIsEmpty, err)

	// Test get when the same key changes
	err = couloyDB.Put(bytes.IntToBytes(2), bytes.RandomBytes(64))
	v1, err := couloyDB.Get(bytes.IntToBytes(2))
	assert.Nil(t, err)
	err = couloyDB.Put(bytes.IntToBytes(2), bytes.RandomBytes(64))
	v2, err := couloyDB.Get(bytes.IntToBytes(2))
	assert.Nil(t, err)
	assert.NotEqual(t, v1, v2)

	// Test get after delete key
	err = couloyDB.Del(bytes.IntToBytes(2))
	assert.Nil(t, err)
	value, err = couloyDB.Get(bytes.IntToBytes(2))
	assert.Nil(t, value)
	assert.NotNil(t, err)
	assert.Equal(t, public.ErrKeyNotFound, err)
}

func TestDB_Put_Get_In_Parallel(t *testing.T) {
	options := DefaultOptions()
	options.DataFileSize = 8 * 1024 * 1024
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
			err := couloyDB.Put(bytes.IntToBytes(id), bytes.IntToBytes(id))
			assert.Nil(t, err)
		}(id)
	}

	w.Wait()

	// Test whether the 200 put workers just now are successful
	w.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func(id int) {
			defer w.Done()
			value, err := couloyDB.Get(bytes.IntToBytes(id))
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
			key := bytes.IntToBytes(id)
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
			key := bytes.IntToBytes(id)
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
	couloyDB, err := NewCouloyDB(options)
	defer destroyCouloyDB(couloyDB)
	assert.Nil(t, err)
	assert.NotNil(t, couloyDB)

	err = couloyDB.Put(bytes.IntToBytes(1), bytes.IntToBytes(1))
	assert.Nil(t, err)

	// Test del normally
	err = couloyDB.Del(bytes.IntToBytes(1))
	assert.Nil(t, err)
	value, err := couloyDB.Get(bytes.IntToBytes(2))
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
			key := bytes.IntToBytes(id)
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
			key := bytes.IntToBytes(id)
			value, err := couloyDB.Get(key)
			assert.Nil(t, err)
			assert.Equal(t, value, append(key, fill...))
		}(id)
	}
	w.Wait()
}
