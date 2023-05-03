package dict

import (
	"github.com/Kirov7/CouloyDB"
	"github.com/Kirov7/CouloyDB/public"
	"log"
)

type CouloyDict struct {
	couloy *CouloyDB.DB
}

// NewCouloyDict makes a new map
func NewCouloyDict(opt CouloyDB.Options) *CouloyDict {
	db, err := CouloyDB.NewCouloyDB(opt)
	if err != nil {
		log.Fatal("New CouloyDB ERROR: ", err)
	}
	return &CouloyDict{couloy: db}
}

func (cdb *CouloyDict) Get(key string) (val []byte, exists bool) {
	value, err := cdb.couloy.Get([]byte(key))
	if err == public.ErrKeyNotFound {
		return nil, false
	}
	if err != nil {
		panic("CouloyDB internal ERROR: " + err.Error())
	}
	return value, true
}

func (cdb *CouloyDict) Len() int {
	return cdb.couloy.Size()
}

func (cdb *CouloyDict) Put(key string, val []byte) (result int) {
	err := cdb.couloy.Put([]byte(key), val)
	if err != nil {
		panic("CouloyDB internal ERROR: " + err.Error())
	}
	return 1
}

func (cdb *CouloyDict) PutIfAbsent(key string, val []byte) (result int) {
	_, err := cdb.couloy.Get([]byte(key))
	if err == public.ErrKeyNotFound {
		return cdb.Put(key, val)
	}
	if err != nil {
		panic("CouloyDB internal ERROR: " + err.Error())
	}
	return 0
}

func (cdb *CouloyDict) PutIfExists(key string, val []byte) (result int) {
	exist, err := cdb.couloy.IsExist([]byte(key))
	if exist {
		return cdb.Put(key, val)
	}

	if err != nil && err != public.ErrKeyNotFound {
		panic("CouloyDB internal ERROR: " + err.Error())
	}
	return 0
}

func (cdb *CouloyDict) Remove(key string) (result int) {
	err := cdb.couloy.Del([]byte(key))
	if err != nil {
		panic("CouloyDB internal ERROR: " + err.Error())
	}
	return 1
}

func (cdb *CouloyDict) ForEach(consumer Consumer) {
	err := cdb.couloy.Fold(consumer)
	if err != nil {
		panic("CouloyDB internal ERROR: " + err.Error())
	}
}

func (cdb *CouloyDict) Keys() []string {
	keys := cdb.couloy.ListKeys()

	return byteToStringSlice(keys)
}

func (cdb *CouloyDict) RandomKeys(limit int) []string {
	//TODO implement me
	panic("implement me")
}

func (cdb *CouloyDict) RandomDistinctKeys(limit int) []string {
	//TODO implement me
	panic("implement me")
}

func (cdb *CouloyDict) Exist(key string) bool {
	exist, err := cdb.couloy.IsExist([]byte(key))
	if exist {
		return true
	}

	if err != nil && err != public.ErrKeyNotFound {
		panic("CouloyDB internal ERROR: " + err.Error())
	}
	return false
}

func (cdb *CouloyDict) Clear() {
	err := cdb.couloy.Clear()
	if err != nil {
		panic("CouloyDB internal ERROR: " + err.Error())
	}
}

func byteToStringSlice(input [][]byte) []string {
	output := make([]string, len(input))
	for i, b := range input {
		output[i] = string(b)
	}
	return output
}
