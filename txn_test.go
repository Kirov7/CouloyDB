package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/bytex"
	"github.com/Kirov7/CouloyDB/public/utils/wait"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestTxn_SetNX(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err := txn.Set(bytex.GetTestKey(0), bytex.RandomBytes(24))
		assert.Nil(t, err)

		err = txn.SetNX(bytex.GetTestKey(0), bytex.RandomBytes(24))
		assert.NotNil(t, err)
		assert.Equal(t, public.ErrKeyExist, err)

		err = txn.SetNX(bytex.GetTestKey(1), bytex.RandomBytes(24))
		assert.Nil(t, err)

		return nil
	})

	assert.Nil(t, err)
}

func TestTxn_StrLen(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err := txn.Set(bytex.GetTestKey(0), bytex.RandomBytes(24))
		assert.Nil(t, err)

		strLen, err := txn.StrLen(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, 24, strLen)

		return nil
	})

	assert.Nil(t, err)
}

func TestTxn_GetSet(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err := txn.Set(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)

		oldVal, err := txn.GetSet(bytex.GetTestKey(0), bytex.GetTestKey(1))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(0), oldVal)

		newVal, err := txn.Get(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(1), newVal)

		return nil
	})
}

func TestTxn_Incr(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err := txn.Set(bytex.GetTestKey(0), []byte(strconv.Itoa(1)))
		assert.Nil(t, err)

		incr, err := txn.Incr(bytex.GetTestKey(0))
		assert.Equal(t, 2, incr)
		assert.Nil(t, err)

		incr, err = txn.Incr(bytex.GetTestKey(1))
		assert.Nil(t, err)
		assert.Equal(t, 1, incr)

		incr, err = txn.IncrBy(bytex.GetTestKey(0), 3)
		assert.Nil(t, err)
		assert.Equal(t, 5, incr)

		value, err := txn.Get(bytex.GetTestKey(0))
		assert.Nil(t, err)

		i, err := strconv.Atoi(string(value))
		assert.Nil(t, err)
		assert.Equal(t, 5, i)

		return nil
	})
}

func TestTxn_Decr(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err := txn.Set(bytex.GetTestKey(0), []byte(strconv.Itoa(1)))
		assert.Nil(t, err)

		decr, err := txn.Decr(bytex.GetTestKey(0))
		assert.Equal(t, 0, decr)
		assert.Nil(t, err)

		decr, err = txn.Decr(bytex.GetTestKey(1))
		assert.Nil(t, err)
		assert.Equal(t, -1, decr)

		decr, err = txn.DecrBy(bytex.GetTestKey(0), 3)
		assert.Nil(t, err)
		assert.Equal(t, -3, decr)

		value, err := txn.Get(bytex.GetTestKey(0))
		assert.Nil(t, err)

		i, err := strconv.Atoi(string(value))
		assert.Equal(t, -3, i)

		return nil
	})
}

func TestTxn_Append(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err := txn.Set(bytex.GetTestKey(0), []byte("hello"))
		assert.Nil(t, err)

		err = txn.Append(bytex.GetTestKey(0), []byte(", world"))
		assert.Nil(t, err)

		value, err := txn.Get(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, []byte("hello, world"), value)

		err = txn.Append(bytex.GetTestKey(1), []byte("couloy"))
		assert.Nil(t, err)

		value, err = txn.Get(bytex.GetTestKey(1))
		assert.Nil(t, err)
		assert.Equal(t, []byte("couloy"), value)

		return nil
	})
}

func TestTxn_MSet(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err := txn.MSet([][]byte{
			bytex.GetTestKey(0), bytex.GetTestKey(0),
			bytex.GetTestKey(1), bytex.GetTestKey(1),
			bytex.GetTestKey(2), bytex.GetTestKey(2),
		})
		assert.Nil(t, err)

		values, err := txn.MGet([][]byte{bytex.GetTestKey(0), bytex.GetTestKey(1), bytex.GetTestKey(2)})
		assert.Nil(t, err)

		for i := 0; i < 3; i++ {
			assert.Equal(t, bytex.GetTestKey(i), values[i])
		}

		values, err = txn.MGet([][]byte{bytex.GetTestKey(3)})
		assert.Nil(t, err)
		assert.Nil(t, values[0])

		return err
	})

	assert.Nil(t, err)
}

func TestDB_RWTransaction(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	t.Run("RWTransaction", func(t *testing.T) {
		wg := wait.NewWait()
		wg.Add(2)

		go func() {
			defer wg.Done()

			err := db.RWTransaction(false, func(txn *Txn) error {
				err := txn.Set([]byte("key"), []byte("initial_value"))
				assert.Nil(t, err)

				value, err := txn.Get([]byte("key"))
				assert.Nil(t, err)
				assert.Equal(t, []byte("initial_value"), value)

				// simulate waiting to ensure that the txn of goroutine 2 is committed first
				time.Sleep(200 * time.Millisecond)

				value, err = txn.Get([]byte("key"))
				assert.Nil(t, err)
				assert.Equal(t, []byte("initial_value"), value)

				return nil
			})

			// since txn of goroutine 2 is committed, txn of goroutine 1 can't commit successfully.
			// txn of goroutine 2 have to roll back.
			assert.NotNil(t, err)
			assert.Equal(t, err, public.ErrTransactionConflict)
		}()

		go func() {
			defer wg.Done()

			// simulate waiting to ensure that the txn of goroutine 1 have started
			time.Sleep(100 * time.Millisecond)

			err := db.RWTransaction(false, func(txn *Txn) error {
				// put the same key with txn of goroutine 1
				err := txn.Set([]byte("key"), []byte("new_value"))
				assert.Nil(t, err)

				return nil
			})
			assert.Nil(t, err)
		}()

		wg.Wait()

		finalValue, err := db.Get([]byte("key"))
		assert.Nil(t, err)
		// txn of goroutine 2 is committed successfully
		assert.Equal(t, []byte("new_value"), finalValue)
	})
}

func TestDB_SerialTransaction_With_RWTransaction(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	t.Run("SerialTransaction_With_RWTransaction", func(t *testing.T) {
		wg := wait.NewWait()
		wg.Add(2)

		go func() {
			defer wg.Done()

			err := db.RWTransaction(false, func(txn *Txn) error {
				err := txn.Set([]byte("key"), []byte("initial_value"))
				assert.Nil(t, err)

				value, err := txn.Get([]byte("key"))
				assert.Nil(t, err)
				assert.Equal(t, []byte("initial_value"), value)

				// simulate waiting to ensure that the txn of goroutine 2 already began
				// once serial txn began, the rw txn can't commit
				time.Sleep(200 * time.Millisecond)

				value, err = txn.Get([]byte("key"))
				assert.Nil(t, err)
				assert.Equal(t, []byte("initial_value"), value)

				return nil
			})

			// since txn of goroutine 2 is committed, txn of goroutine 1 can't commit successfully.
			// txn of goroutine 2 have to roll back.
			assert.NotNil(t, err)
			assert.Equal(t, err, public.ErrTransactionConflict)
		}()

		go func() {
			defer wg.Done()

			// simulate waiting to ensure that the txn of goroutine 1 have started
			time.Sleep(100 * time.Millisecond)

			err := db.SerialTransaction(false, func(txn *Txn) error {
				// put the same key with txn of goroutine 1
				err := txn.Set([]byte("key"), []byte("new_value"))
				assert.Nil(t, err)

				return nil
			})
			assert.Nil(t, err)
		}()

		wg.Wait()

		finalValue, err := db.Get([]byte("key"))
		assert.Nil(t, err)
		// txn of goroutine 2 is committed successfully
		assert.Equal(t, []byte("new_value"), finalValue)
	})
}
