package meta

import (
	"bytes"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/google/btree"
	"sort"
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

func (bt *BTree) Put(key []byte, pos *data.LogPos) bool {
	item := &Item{
		Key: key,
		Pos: pos,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()

	bt.tree.ReplaceOrInsert(item)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogPos {
	item := &Item{
		Key: key,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()

	value := bt.tree.Get(item)
	if value == nil {
		return nil
	}
	return value.(*Item).Pos
}

func (bt *BTree) Del(key []byte) bool {
	item := &Item{
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

func (bt *BTree) Count() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

type btreeIterator struct {
	currentIndex int
	reverse      bool
	values       []*Item
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// store all data to this slice
	saveValuesFunc := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValuesFunc)
	} else {
		tree.Ascend(saveValuesFunc)
	}

	return &btreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}

func (bi *btreeIterator) Rewind() {
	bi.currentIndex = 0
}

func (bi *btreeIterator) Seek(key []byte) {
	if bi.reverse {
		bi.currentIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].Key, key) <= 0
		})
	} else {
		bi.currentIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].Key, key) >= 0
		})
	}

}

func (bi *btreeIterator) Next() {
	bi.currentIndex += 1
}

func (bi *btreeIterator) Valid() bool {
	return bi.currentIndex < len(bi.values)
}

func (bi *btreeIterator) Key() []byte {
	return bi.values[bi.currentIndex].Key
}

func (bi *btreeIterator) Value() *data.LogPos {
	return bi.values[bi.currentIndex].Pos
}

func (bi *btreeIterator) Close() {
	bi.values = nil
}
