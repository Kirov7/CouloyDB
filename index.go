package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/meta"
)

type hashIndex map[string]meta.MemTable

type listIndex struct {
	metaIndex meta.MemTable // key to list metadata(head seq and tail seq)
	dataIndex map[string]meta.MemTable
}

type index struct {
	strIndex  meta.MemTable
	hashIndex hashIndex
	listIndex listIndex
}

func (i *index) getStrIndex() meta.MemTable {
	return i.strIndex
}

func (i *index) getHashIndex(key string) (meta.MemTable, bool) {
	if idx, ok := i.hashIndex[key]; ok {
		return idx, ok
	}
	return nil, false
}

func (i *index) setHashIndex(key string, memTable meta.MemTable) {
	i.hashIndex[key] = memTable
}

func (i *index) getListMetaIndex() meta.MemTable {
	return i.listIndex.metaIndex
}

func (i *index) getListDataIndex(key string) (meta.MemTable, bool) {
	if idx, ok := i.listIndex.dataIndex[key]; ok {
		return idx, ok
	}
	return nil, false
}

func (i *index) setListDataIndex(key string, memTable meta.MemTable) {
	i.listIndex.dataIndex[key] = memTable
}
