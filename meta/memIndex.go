package meta

import (
	"bytes"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta/collections"
	"github.com/google/btree"
)

type MemIndex interface {
	// Put Stores the Pos information for key pairs in the index
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get Retrieve the Pos information based on the key
	Get(key []byte) *data.LogRecordPos

	// Del Delete the Pos information based on the key
	Del(key []byte) bool
}

type IndexType = int8

const (
	Btree IndexType = iota
)

func NewIndexer(typ IndexType) MemIndex {
	switch typ {
	case Btree:
		return collections.NewBTree()
	default:
		return collections.NewBTree()
	}
}

type Item struct {
	Key []byte
	Pos *data.LogRecordPos
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.Key, bi.(*Item).Key) == -1
}
