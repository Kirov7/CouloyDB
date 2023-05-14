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
	return newArtIterator(a, reverse)
}

type artIterator struct {
	currentIndex int
	reverse      bool
	keys         [][]byte

	art      *AdaptiveRadixTree
	iterator art.Iterator
	currItem Item
}

func newArtIterator(artree *AdaptiveRadixTree, reverse bool) *artIterator {
	if !reverse {
		return &artIterator{
			reverse:  reverse,
			art:      artree,
			iterator: artree.tree.Iterator(),
		}
	}

	// store all data to this slice
	idx := artree.tree.Size() - 1

	keys := make([][]byte, artree.Count())
	saveValues := func(node art.Node) bool {
		keys[idx] = node.Key()
		idx--
		return true
	}

	artree.tree.ForEach(saveValues)
	return &artIterator{
		currentIndex: 0,
		reverse:      reverse,
		keys:         keys,
	}
}

func (ai *artIterator) Rewind() {
	if !ai.reverse {
		ai.iterator = ai.art.tree.Iterator()
		return
	}
	ai.currentIndex = 0
}

func (ai *artIterator) Seek(key []byte) bool {
	if !ai.reverse {
		if v := ai.art.Get(key); v != nil {
			ai.currItem = Item{
				Key: key,
				Pos: v,
			}
			return true
		}
		return false
	}

	index := 0
	if ai.reverse {
		index = sort.Search(len(ai.keys), func(i int) bool {
			return bytes.Compare(ai.keys[i], key) <= 0
		})
	} else {
		index = sort.Search(len(ai.keys), func(i int) bool {
			return bytes.Compare(ai.keys[i], key) >= 0
		})
	}
	if index < 0 {
		return false
	} else {
		ai.currentIndex = index
		return true
	}
}

func (ai *artIterator) Next() {
	if !ai.reverse {
		next, _ := ai.iterator.Next()
		ai.currItem = Item{
			Key: next.Key(),
			Pos: next.Value().(*data.LogPos),
		}
		return
	}

	ai.currentIndex += 1
}

func (ai *artIterator) Valid() bool {
	if !ai.reverse {
		return ai.iterator.HasNext()
	}

	return ai.currentIndex < len(ai.keys)
}

func (ai *artIterator) Key() []byte {
	if !ai.reverse {
		return ai.currItem.Key
	}

	return ai.keys[ai.currentIndex]
}

func (ai *artIterator) Value() *data.LogPos {
	if !ai.reverse {
		return ai.currItem.Pos
	}

	return ai.art.Get(ai.keys[ai.currentIndex])
}

func (ai *artIterator) Close() {
	ai.art = nil
	ai.keys = nil
}
