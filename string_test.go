package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/bytex"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFacade_SetNX(t *testing.T) {
	options := DefaultOptions()
	options.SyncWrites = false
	db, err := NewCouloyDB(options)

	assert.NotNil(t, db)
	assert.Nil(t, err)

	defer destroyCouloyDB(db)

	facade := &Facade{db: db}

	err = facade.SetNX(bytex.GetTestKey(0), bytex.RandomBytes(24))
	assert.Nil(t, err)

	err = facade.SetNX(bytex.GetTestKey(0), bytex.RandomBytes(24))
	assert.NotNil(t, err)
	assert.Equal(t, public.ErrKeyExist, err)

	err = facade.Del(bytex.GetTestKey(0))
	assert.Nil(t, err)

	err = facade.SetNX(bytex.GetTestKey(0), bytex.RandomBytes(24))
	assert.Nil(t, err)
}

func TestFacade_GetSet(t *testing.T) {
	options := DefaultOptions()
	options.SyncWrites = false
	db, err := NewCouloyDB(options)

	assert.NotNil(t, db)
	assert.Nil(t, err)

	defer destroyCouloyDB(db)

	facade := &Facade{db: db}

	err = facade.Set(bytex.GetTestKey(0), bytex.GetTestKey(0))

	oldVal, err := facade.GetSet(bytex.GetTestKey(0), bytex.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, bytex.GetTestKey(0), oldVal)

	newVal, err := facade.Get(bytex.GetTestKey(0))
	assert.Nil(t, err)
	assert.Equal(t, bytex.GetTestKey(1), newVal)

	oldVal, err = facade.GetSet(bytex.GetTestKey(1), bytex.GetTestKey(1))
	assert.Nil(t, err)
	assert.Nil(t, oldVal)

	newVal, err = facade.Get(bytex.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, bytex.GetTestKey(1), newVal)
}

func TestFacade_StrLen(t *testing.T) {
	options := DefaultOptions()
	options.SyncWrites = false
	db, err := NewCouloyDB(options)

	assert.NotNil(t, db)
	assert.Nil(t, err)

	defer destroyCouloyDB(db)

	facade := &Facade{db: db}

	err = facade.Set(bytex.GetTestKey(0), bytex.RandomBytes(24))
	assert.Nil(t, err)

	strLen, err := facade.StrLen(bytex.GetTestKey(0))
	assert.Nil(t, err)
	assert.Equal(t, 24, strLen)

	strLen, err = facade.StrLen(bytex.GetTestKey(1))
	assert.NotNil(t, err)
	assert.Equal(t, public.ErrKeyNotFound, err)
	assert.Equal(t, 0, strLen)
}
