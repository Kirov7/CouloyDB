package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/bytex"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTxn_List_Push(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		// 0
		err := txn.LPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(0)})
		assert.Nil(t, err)

		// 1 0
		err = txn.LPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(1)})
		assert.Nil(t, err)

		// 3 2 1 0
		err = txn.LPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(2), bytex.GetTestKey(3)})
		assert.Nil(t, err)

		// 3 2 1 0 4
		err = txn.RPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(4)})
		assert.Nil(t, err)

		// 3 2 1 0 4 5 6
		err = txn.RPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(5), bytex.GetTestKey(6)})
		assert.Nil(t, err)

		// 2 1 0 4 5 6
		value, err := txn.LPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(3), value)

		// 1 0 4 5 6
		value, err = txn.LPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(2), value)

		// 1 0 4 5
		value, err = txn.RPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(6), value)

		return err
	})

	assert.Nil(t, err)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		// 1 0 4 5 7
		err := txn.RPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(7)})
		assert.Nil(t, err)

		// 0 4 5 7
		value, err := txn.LPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(1), value)

		// 4 5 7
		value, err = txn.LPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(0), value)

		// 4 5
		value, err = txn.RPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(7), value)

		// 5
		value, err = txn.LPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(4), value)

		// nil
		value, err = txn.RPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(5), value)

		return err
	})

	assert.Nil(t, err)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		value, err := txn.LPop(bytex.GetTestKey(0))
		assert.Nil(t, value)
		assert.NotNil(t, err)
		assert.Equal(t, public.ErrListIsEmpty, err)

		return err
	})

	assert.Equal(t, public.ErrListIsEmpty, err)
}

func TestTxn_List_Restart(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		// 0
		err := txn.LPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(0)})
		assert.Nil(t, err)

		// 1 0
		err = txn.LPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(1)})
		assert.Nil(t, err)

		// 3 2 1 0
		err = txn.LPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(2), bytex.GetTestKey(3)})
		assert.Nil(t, err)

		// 3 2 1 0 4
		err = txn.RPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(4)})
		assert.Nil(t, err)

		// 3 2 1 0 4 5 6
		err = txn.RPush(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(5), bytex.GetTestKey(6)})
		assert.Nil(t, err)

		return err
	})

	err = db.Close()
	assert.Nil(t, err)

	db, err = NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		// 2 1 0 4 5 6
		value, err := txn.LPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(3), value)

		// 1 0 4 5 6
		value, err = txn.LPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(2), value)

		// 1 0 4 5
		value, err = txn.RPop(bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(6), value)

		return err
	})

	assert.Nil(t, err)
}
