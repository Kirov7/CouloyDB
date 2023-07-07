package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/wait"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

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
				err := txn.Put([]byte("key"), []byte("initial_value"))
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
				err := txn.Put([]byte("key"), []byte("new_value"))
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
				err := txn.Put([]byte("key"), []byte("initial_value"))
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
				err := txn.Put([]byte("key"), []byte("new_value"))
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
