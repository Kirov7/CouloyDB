package meta

import (
	"bytes"
	"github.com/Kirov7/CouloyDB/data"
	art "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	tree art.Tree
	lock *sync.RWMutex
}

func NewAdaptiveRadixTree() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: art.New(),
		lock: new(sync.RWMutex),
	}
}

func (a *AdaptiveRadixTree) Put(key []byte, pos *data.LogPos) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	_, updated := a.tree.Insert(key, pos)
	return updated
}

func (a *AdaptiveRadixTree) Get(key []byte) *data.LogPos {
	a.lock.RLock()
	defer a.lock.RUnlock()
	value, ok := a.tree.Search(key)
	if !ok {
		return nil
	}
	return value.(*data.LogPos)
}

func (a *AdaptiveRadixTree) Del(key []byte) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	_, deleted := a.tree.Delete(key)
	return deleted
}

func (a *AdaptiveRadixTree) Count() int {
	a.lock.RLock()
	defer a.lock.Unlock()
	size := a.tree.Size()
	return size
}

func (a *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	a.lock.RLock()
	defer a.lock.Unlock()
	return newArtIterator(a.tree, reverse)
}

type artIterator struct {
	currentIndex int
	reverse      bool
	values       []*Item
}

func newArtIterator(tree art.Tree, reverse bool) *artIterator {
	var idx int

	// store all data to this slice
	if reverse {
		idx = tree.Size() - 1
	}

	values := make([]*Item, tree.Size())
	saveValues := func(node art.Node) bool {
		item := &Item{
			Key: node.Key(),
			Pos: node.Value().(*data.LogPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}

func (bi *artIterator) Rewind() {
	bi.currentIndex = 0
}

func (bi *artIterator) Seek(key []byte) bool {
	index := 0
	if bi.reverse {
		index = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].Key, key) <= 0
		})
	} else {
		index = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].Key, key) >= 0
		})
	}
	if index < 0 {
		return false
	} else {
		bi.currentIndex = index
		return true
	}
}

func (bi *artIterator) Next() {
	bi.currentIndex += 1
}

func (bi *artIterator) Valid() bool {
	return bi.currentIndex < len(bi.values)
}

func (bi *artIterator) Key() []byte {
	return bi.values[bi.currentIndex].Key
}

func (bi *artIterator) Value() *data.LogPos {
	return bi.values[bi.currentIndex].Pos
}

func (bi *artIterator) Close() {
	bi.values = nil
}
