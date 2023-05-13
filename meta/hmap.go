package meta

import (
	"bytes"
	"fmt"
	"github.com/Kirov7/CouloyDB/data"
	"sort"
	"sync"
)

type HashMap struct {
	sm *sync.Map
}

func NewHashMap() *HashMap {
	return &HashMap{sm: &sync.Map{}}
}

func (h *HashMap) Put(key []byte, pos *data.LogPos) bool {
	_, flag := h.sm.Load(string(key))
	h.sm.Store(string(key), pos)
	return flag
}

func (h *HashMap) Get(key []byte) *data.LogPos {
	value, ok := h.sm.Load(string(key))
	if ok {
		return value.(*data.LogPos)
	}
	return nil
}

func (h *HashMap) Del(key []byte) bool {
	_, ok := h.sm.Load(string(key))
	if ok {
		h.sm.Delete(string(key))
	}
	return ok
}

func (h *HashMap) Iterator(reverse bool) Iterator {
	isEmpty := true
	h.sm.Range(func(key, value interface{}) bool {
		isEmpty = false
		return false
	})
	if isEmpty {
		return nil
	}
	return newHashMapIterator(h.sm, reverse)
}

func (h *HashMap) Count() int {
	count := 0
	h.sm.Range(func(key, value any) bool {
		count++
		return true
	})
	return count
}

type HashMapIterator struct {
	currentIndex int
	reverse      bool
	values       []*Item
}

func newHashMapIterator(m *sync.Map, reverse bool) *HashMapIterator {
	values := make([]*Item, 0)
	m.Range(func(key, value any) bool {
		values = append(values, &Item{Key: []byte(key.(string)), Pos: value.(*data.LogPos)})
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

func (hi *HashMapIterator) Seek(key []byte) bool {
	index := 0
	if hi.reverse {
		index = sort.Search(len(hi.values), func(i int) bool {
			return bytes.Compare(hi.values[i].Key, key) <= 0
		})
	} else {
		index = sort.Search(len(hi.values), func(i int) bool {
			return bytes.Compare(hi.values[i].Key, key) >= 0
		})
	}
	fmt.Println("index:", index)
	fmt.Println(len(hi.values))
	if index < 0 || index >= len(hi.values) {
		return false
	}
	hi.currentIndex = index
	return true
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
