package meta

import (
	"CouloyDB/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	// Put Stores the pos information for key pairs in the index
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get Retrieve the pos information based on the key
	Get(key []byte) *data.LogRecordPos

	// Del Delete the pos information based on the key
	Del(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}
