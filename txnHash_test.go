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

		return err
	})

	exceptedData[string(bytex.GetTestKey(3))] = bytex.GetTestKey(3)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		err = txn.HSet(bytex.GetTestKey(0), bytex.GetTestKey(3), bytex.GetTestKey(3))
		assert.Nil(t, err)

		allKey, allValue, err := txn.HGetAll(bytex.GetTestKey(0))
		for i, key := range allKey {
			assert.Equal(t, exceptedData[string(key)], allValue[i])
		}

		return err
	})
}

func TestTxn_HMSet(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	err = db.SerialTransaction(false, func(txn *Txn) error {
		return txn.HMSet(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(0), bytex.GetTestKey(0),
			bytex.GetTestKey(1), bytex.GetTestKey(1),
			bytex.GetTestKey(2), bytex.GetTestKey(2)})
	})

	err = db.SerialTransaction(true, func(txn *Txn) error {
		values, err := txn.HMGet(bytex.GetTestKey(0), [][]byte{bytex.GetTestKey(0),
			bytex.GetTestKey(1), bytex.GetTestKey(2), bytex.GetTestKey(3)})

		assert.Nil(t, err)
		for i, value := range values {
			if i < 3 {
				assert.Equal(t, bytex.GetTestKey(i), value)
			} else {
				assert.Nil(t, value)
			}
		}

		return err
	})

	assert.Nil(t, err)

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

func TestTxn_HStrLen(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)
	var StrLen int64
	err = db.SerialTransaction(false, func(txn *Txn) error {
		err = txn.HSet(bytex.GetTestKey(0), bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)
		v, err := txn.HGet(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)
		StrLen, err = txn.HStrLen(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, StrLen, int64(len(v)))
		return err
	})
	assert.Nil(t, err)
	err = db.SerialTransaction(false, func(txn *Txn) error {
		v, err := txn.HGet(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, StrLen, int64(len(v)))
		StrLen, err = txn.HStrLen(bytex.GetTestKey(0), bytex.GetTestKey(0))
		assert.Nil(t, err)
		assert.Equal(t, StrLen, int64(len(v)))
		return err
	})
	assert.Nil(t, err)

}

func TestTxn_HKEYS_HVALUES(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)
	var files, values, filesLabel, valuesLabel [][]byte
	err = db.SerialTransaction(false, func(txn *Txn) error {
		err = txn.HSet(bytex.GetTestKey(0), []byte("1"), []byte("1"))
		assert.Nil(t, err)
		err = txn.HSet(bytex.GetTestKey(0), []byte("2"), []byte("2"))
		assert.Nil(t, err)
		err = txn.HSet(bytex.GetTestKey(0), []byte("3"), []byte("3"))
		assert.Nil(t, err)
		files, err = txn.HKeys(bytex.GetTestKey(0))
		bytex.BytesSort(files)
		assert.Nil(t, err)
		values, err = txn.HValues(bytex.GetTestKey(0))
		bytex.BytesSort(values)
		assert.Nil(t, err)
		filesLabel, valuesLabel, err = txn.HGetAll(bytex.GetTestKey(0))
		bytex.BytesSort(filesLabel)
		bytex.BytesSort(valuesLabel)
		assert.Nil(t, err)
		assert.Equal(t, files, filesLabel)
		assert.Equal(t, values, valuesLabel)
		err = txn.HDel(bytex.GetTestKey(0), []byte("1"))
		assert.Nil(t, err)
		files, err = txn.HKeys(bytex.GetTestKey(0))
		bytex.BytesSort(files)
		assert.Nil(t, err)
		values, err = txn.HValues(bytex.GetTestKey(0))
		bytex.BytesSort(values)
		assert.Nil(t, err)
		filesLabel, valuesLabel, err = txn.HGetAll(bytex.GetTestKey(0))
		bytex.BytesSort(filesLabel)
		bytex.BytesSort(valuesLabel)
		assert.Nil(t, err)
		assert.Equal(t, files, filesLabel)
		assert.Equal(t, values, valuesLabel)
		return err
	})
	assert.Nil(t, err)
	err = db.SerialTransaction(false, func(txn *Txn) error {
		files, err = txn.HKeys(bytex.GetTestKey(0))
		bytex.BytesSort(files)
		assert.Nil(t, err)
		values, err = txn.HValues(bytex.GetTestKey(0))
		bytex.BytesSort(values)
		assert.Nil(t, err)
		assert.Equal(t, files, filesLabel)
		assert.Equal(t, values, valuesLabel)
		return err
	})
	assert.Nil(t, err)
}

func TestTxn_HLEN(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)
	var StrLen int64
	byteExample := [][]byte{[]byte("01"), []byte("12"), []byte("23")}
	err = db.SerialTransaction(false, func(txn *Txn) error {
		err = txn.HSet(byteExample[0], byteExample[0], byteExample[0])
		assert.Nil(t, err)
		err = txn.HSet(byteExample[0], byteExample[1], byteExample[1])
		assert.Nil(t, err)
		err = txn.HSet(byteExample[0], byteExample[2], byteExample[2])
		assert.Nil(t, err)
		//new test
		StrLen, err = txn.HLen(byteExample[0])
		assert.Nil(t, err)
		StrLenLabel, _, err := txn.HGetAll(byteExample[0])
		assert.Nil(t, err)
		assert.Equal(t, StrLen, int64(len(StrLenLabel)))
		//del test
		err = txn.HDel(byteExample[0], byteExample[2])
		assert.Nil(t, err)
		StrLen, err = txn.HLen(byteExample[0])
		assert.Nil(t, err)
		StrLenLabel, _, err = txn.HGetAll(byteExample[0])
		assert.Nil(t, err)
		assert.Equal(t, StrLen, int64(len(StrLenLabel)))
		//update test
		err = txn.HSet(byteExample[0], byteExample[0], byteExample[0])
		StrLen, err = txn.HLen(byteExample[0])
		assert.Nil(t, err)
		StrLenLabel, _, err = txn.HGetAll(byteExample[0])
		assert.Nil(t, err)
		assert.Equal(t, StrLen, int64(len(StrLenLabel)))
		return err
	})
	assert.Nil(t, err)
	err = db.SerialTransaction(false, func(txn *Txn) error {
		StrLenLabel, _, err := txn.HGetAll(byteExample[0])
		assert.Nil(t, err)
		assert.Equal(t, StrLen, int64(len(StrLenLabel)))
		return err
	})
	assert.Nil(t, err)
}
