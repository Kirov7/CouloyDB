package collections

import (
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta"
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
	item := &meta.Item{
		Key: key,
		Pos: pos,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()

	bt.tree.ReplaceOrInsert(item)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	item := &meta.Item{
		Key: key,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()

	value := bt.tree.Get(item)
	if value == nil {
		return nil
	}
	return value.(*meta.Item).Pos
}

func (bt *BTree) Del(key []byte) bool {
	item := &meta.Item{
		Key: key,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()

	value := bt.tree.Delete(item)
	if value == nil {
		return false
	}
	return true
}
