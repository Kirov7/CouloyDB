package CouloyDB

import "github.com/Kirov7/CouloyDB/meta"

type index struct {
	strIndex  meta.MemTable
	hashIndex map[string]meta.MemTable
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
