package meta

import (
	"bytes"
	"github.com/Kirov7/CouloyDB/data"
	"sort"
	"sync"
)

type HashMap struct {
	hmap *sync.Map
}

func NewHashMap() *HashMap {
	return &HashMap{hmap: &sync.Map{}}
}

func (h *HashMap) Put(key []byte, pos *data.LogPos) bool {
	_, flag := h.hmap.Load(string(key))
	h.hmap.Store(string(key), pos)
	return flag
}

func (h *HashMap) Get(key []byte) *data.LogPos {
	value, ok := h.hmap.Load(string(key))
	if ok {
		return value.(*data.LogPos)
	}
	return nil
}

func (h *HashMap) Del(key []byte) bool {
	_, ok := h.hmap.Load(string(key))
	if ok {
		h.hmap.Delete(string(key))
	}
	return ok
}

func (h *HashMap) Iterator(reverse bool) Iterator {
	isEmpty := true
	h.hmap.Range(func(key, value interface{}) bool {
		isEmpty = false
		return false
	})
	if isEmpty {
		return nil
	}
	return newHashMapIterator(h, reverse)
}

func (h *HashMap) Count() int {
	count := 0
	h.hmap.Range(func(key, value any) bool {
		count++
		return true
	})
	return count
}

type HashMapIterator struct {
	currentIndex int
	reverse      bool
	keys         [][]byte

	hmap *HashMap
}

func newHashMapIterator(m *HashMap, reverse bool) *HashMapIterator {
	keys := make([][]byte, 0)
	m.hmap.Range(func(key, value any) bool {
		keys = append(keys, []byte(key.(string)))
		return true
	})

	if reverse {
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) > 0
		})
	} else {
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) < 0
		})
	}

	return &HashMapIterator{
		currentIndex: 0,
		reverse:      reverse,
		keys:         keys,
	}
}

func (hi *HashMapIterator) Rewind() {
	hi.currentIndex = 0
}

func (hi *HashMapIterator) Seek(key []byte) bool {
	index := 0
	if hi.reverse {
		index = sort.Search(len(hi.keys), func(i int) bool {
			return bytes.Compare(hi.keys[i], key) <= 0
		})
	} else {
		index = sort.Search(len(hi.keys), func(i int) bool {
			return bytes.Compare(hi.keys[i], key) >= 0
		})
	}

	if index < 0 || index >= len(hi.keys) {
		return false
	}
	hi.currentIndex = index
	return true
}

func (hi *HashMapIterator) Next() {
	hi.currentIndex += 1
}

func (hi *HashMapIterator) Valid() bool {
	return hi.currentIndex < len(hi.keys)
}

func (hi *HashMapIterator) Key() []byte {
	return hi.keys[hi.currentIndex]
}

func (hi *HashMapIterator) Value() *data.LogPos {
	return hi.hmap.Get(hi.keys[hi.currentIndex])
}

func (hi *HashMapIterator) Close() {
	hi.keys = nil
}
