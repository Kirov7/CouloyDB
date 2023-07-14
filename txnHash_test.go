package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/bytex"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTxn_HSet(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {

		value, err := txn.HGet(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.NotNil(t, err)
		assert.Equal(t, public.ErrKeyNotFound, err)

		err = txn.HSet(bytex.GetTestKey(0), bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)

		err = txn.HSet(nil, bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.NotNil(t, err)
		assert.Equal(t, public.ErrKeyIsEmpty, err)

		err = txn.HSet(bytex.GetTestKey(0), nil, bytex.GetTestKey(0))
		assert.NotNil(t, err)
		assert.Equal(t, public.ErrKeyIsEmpty, err)

		value, err = txn.HGet(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Equal(t, bytex.GetTestKey(0), value)
		assert.Nil(t, err)

		return err
	})
}

func TestTxn_HDel(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err = txn.HSet(bytex.GetTestKey(0), bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)

		value, err := txn.HGet(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Equal(t, bytex.GetTestKey(0), value)
		assert.Nil(t, err)

		err = txn.HDel(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)

		value, err = txn.HGet(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.NotNil(t, err)
		assert.Equal(t, public.ErrKeyNotFound, err)

		err = txn.HDel(bytex.GetTestKey(0), bytex.GetTestKey(1))
		assert.NotNil(t, err)
		assert.Equal(t, public.ErrKeyNotFound, err)

		err = txn.HDel(bytex.GetTestKey(1), bytex.GetTestKey(1))
		assert.NotNil(t, err)
		assert.Equal(t, public.ErrKeyNotFound, err)

		return nil
	})

	assert.Nil(t, err)
}

func TestTxn_HExist(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err = txn.HSet(bytex.GetTestKey(0), bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)

		exist := txn.HExist(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.True(t, exist)

		err = txn.HDel(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)

		exist = txn.HExist(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.False(t, exist)

		return nil
	})

	assert.Nil(t, err)
}

func TestTxn_HGetAll(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	exceptedData := make(map[string][]byte)
	exceptedData[string(bytex.GetTestKey(0))] = bytex.GetTestKey(0)
	exceptedData[string(bytex.GetTestKey(1))] = bytex.GetTestKey(1)
	exceptedData[string(bytex.GetTestKey(2))] = bytex.GetTestKey(2)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err = txn.HSet(bytex.GetTestKey(0), bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)

		err = txn.HSet(bytex.GetTestKey(0), bytex.GetTestKey(1), bytex.GetTestKey(1))
		assert.Nil(t, err)

		err = txn.HSet(bytex.GetTestKey(0), bytex.GetTestKey(2), bytex.GetTestKey(2))
		assert.Nil(t, err)

		allKey, allValue, err := txn.HGetAll(bytex.GetTestKey(0))
		assert.Nil(t, err)

		for i, key := range allKey {
			assert.Equal(t, exceptedData[string(key)], allValue[i])
		}

		return nil
	})
}

func TestTxn_Hash_Restart(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err = txn.HSet(bytex.GetTestKey(0), bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)

		err = txn.HSet(bytex.GetTestKey(1), bytex.GetTestKey(1), bytex.GetTestKey(1))
		assert.Nil(t, err)

		err = txn.HSet(bytex.GetTestKey(1), bytex.GetTestKey(2), bytex.GetTestKey(2))
		assert.Nil(t, err)

		err = txn.HDel(bytex.GetTestKey(1), bytex.GetTestKey(2))

		return err
	})

	err = db.Close()
	assert.Nil(t, err)

	db, err = NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.SerialTransaction(true, func(txn *Txn) error {
		value, err := txn.HGet(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(0), value)

		value, err = txn.HGet(bytex.GetTestKey(1), bytex.GetTestKey(1))
		assert.Nil(t, err)
		assert.Equal(t, bytex.GetTestKey(1), value)

		value, err = txn.HGet(bytex.GetTestKey(1), bytex.GetTestKey(2))
		assert.NotNil(t, err)
		assert.Equal(t, public.ErrKeyNotFound, err)

		return err
	})

	destroyCouloyDB(db)
}
