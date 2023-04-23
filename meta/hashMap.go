package meta

import (
	"bytes"
	"github.com/Kirov7/CouloyDB/data"
	art "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type Map struct {
	tree *sync.Map
}

func NewMap() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: art.New(),
		lock: new(sync.RWMutex),
	}
}

func (h *Map) Put(key []byte, pos *data.LogPos) bool {
	h.tree.Store(key, pos)
	return true
}

func (h *Map) Get(key []byte) *data.LogPos {
	value, ok := h.tree.Load(key)
	if !ok {
		return nil
	}
	return value.(*data.LogPos)
}

func (h *Map) Del(key []byte) bool {
	h.tree.Delete(key)
	return true
}

func (h *Map) Count() int {
	size := 0
	h.tree.Range(func(_, _ interface{}) bool {
		size++
		return true
	})
	return size
}

func (h *Map) Iterator(reverse bool) Iterator {
	return newMapIterator(h.tree, reverse)
}

type HashMapIterator struct {
	currentIndex int
	reverse      bool
	values       []*Item
}

func newMapIterator(m *sync.Map, reverse bool) *HashMapIterator {

	values := make([]*Item, 0)
	m.Range(func(key, value interface{}) bool {
		item := &Item{
			Key: key.([]byte),
			Pos: value.(*data.LogPos),
		}
		values = append(values, item)
		return true
	})

	if reverse {
		sort.Slice(values, func(i, j int) bool {
			return bytes.Compare(values[i].Key, values[j].Key) > 0
		})
	} else {
		sort.Slice(values, func(i, j int) bool {
			return bytes.Compare(values[i].Key, values[j].Key) < 0
		})
	}

	return &HashMapIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}

func (hi *HashMapIterator) Rewind() {
	hi.currentIndex = 0
}

func (hi *HashMapIterator) Seek(key []byte) {
	if hi.reverse {
		hi.currentIndex = sort.Search(len(hi.values), func(i int) bool {
			return bytes.Compare(hi.values[i].Key, key) <= 0
		})
	} else {
		hi.currentIndex = sort.Search(len(hi.values), func(i int) bool {
			return bytes.Compare(hi.values[i].Key, key) >= 0
		})
	}

}

func (hi *HashMapIterator) Next() {
	hi.currentIndex += 1
}

func (hi *HashMapIterator) Valid() bool {
	return hi.currentIndex < len(hi.values)
}

func (hi *HashMapIterator) Key() []byte {
	return hi.values[hi.currentIndex].Key
}

func (hi *HashMapIterator) Value() *data.LogPos {
	return hi.values[hi.currentIndex].Pos
}

func (hi *HashMapIterator) Close() {
	hi.values = nil
}
