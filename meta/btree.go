package meta

import (
	"CouloyDB/data"
	"github.com/google/btree"
	"sync"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree Init BTree struct
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := &Item{
		key: key,
		pos: pos,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()

	bt.tree.ReplaceOrInsert(item)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	item := &Item{
		key: key,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()

	value := bt.tree.Get(item)
	if value == nil {
		return nil
	}
	return value.(*Item).pos
}

func (bt *BTree) Del(key []byte) bool {
	item := &Item{
		key: key,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()

	value := bt.tree.Delete(item)
	if value == nil {
		return false
	}
	return true
}
