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

	bt.lock.RLock()
	defer bt.lock.RUnlock()

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

type Item struct {
	Key []byte
	Pos *data.LogPos
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.Key, bi.(*Item).Key) == -1
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt, reverse)
}

type btreeIterator struct {
	currentIndex int
	reverse      bool
	keys         [][]byte

	tree *BTree
}

func newBtreeIterator(bt *BTree, reverse bool) *btreeIterator {
	var idx int
	keys := make([][]byte, bt.Count())

	// store all data to this slice
	saveValuesFunc := func(it btree.Item) bool {
		keys[idx] = it.(*Item).Key
		idx++
		return true
	}
	if reverse {
		bt.tree.Descend(saveValuesFunc)
	} else {
		bt.tree.Ascend(saveValuesFunc)
	}

	return &btreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		keys:         keys,
		tree:         bt,
	}
}

func (bi *btreeIterator) Rewind() {
	bi.currentIndex = 0
}

func (bi *btreeIterator) Seek(key []byte) bool {
	index := 0
	if bi.reverse {
		index = sort.Search(len(bi.keys), func(i int) bool {
			return bytes.Compare(bi.keys[i], key) <= 0
		})
	} else {
		index = sort.Search(len(bi.keys), func(i int) bool {
			return bytes.Compare(bi.keys[i], key) >= 0
		})
	}
	if index < 0 {
		return false
	} else {
		bi.currentIndex = index
		return true
	}
}

func (bi *btreeIterator) Next() {
	bi.currentIndex += 1
}

func (bi *btreeIterator) Valid() bool {
	return bi.currentIndex < len(bi.keys)
}

func (bi *btreeIterator) Key() []byte {
	return bi.keys[bi.currentIndex]
}

func (bi *btreeIterator) Value() *data.LogPos {
	return bi.tree.Get(bi.keys[bi.currentIndex])
}

func (bi *btreeIterator) Close() {
	bi.keys = nil
}
