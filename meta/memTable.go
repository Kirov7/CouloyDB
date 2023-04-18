package meta

import (
	"bytes"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/google/btree"
)

type MemTableType = int8

const (
	Btree MemTableType = iota
)

type MemTable interface {
	// Put Stores the Pos information for key pairs in the index
	Put(key []byte, pos *data.LogPos) bool

	// Get Retrieve the Pos information based on the key
	Get(key []byte) *data.LogPos

	// Del Delete the Pos information based on the key
	Del(key []byte) bool

	// Iterator Index iterator
	Iterator(reverse bool) Iterator

	// Count get the num of all the data
	Count() int
}

func NewMemTable(typ MemTableType) MemTable {
	switch typ {
	case Btree:
		return NewBTree()
	default:
		return NewBTree()
	}
}

type Item struct {
	Key []byte
	Pos *data.LogPos
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.Key, bi.(*Item).Key) == -1
}

// Iterator Generic index iterator interface
type Iterator interface {
	Rewind()
	Seek(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() *data.LogPos
	Close()
}