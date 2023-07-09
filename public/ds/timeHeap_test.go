package ds

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTimeHeap(t *testing.T) {

	jh := &TimeHeap{
		heap: h{
			heap:  make([]*Job, 0),
			index: make(map[string]int),
		},
	}

	now := time.Now()

	job1 := NewJob("key1", time.Unix(0, now.Add(time.Second*1).UnixNano()))
	job2 := NewJob("key2", time.Unix(0, now.Add(time.Second*2).UnixNano()))
	job3 := NewJob("key3", time.Unix(0, now.Add(time.Second*3).UnixNano()))

	jh.Push(job1)
	jh.Push(job2)
	jh.Push(job3)

	assert.Equal(t, "key1", jh.Peek().Key)
	assert.Equal(t, 0, jh.heap.index["key1"])

	job4 := NewJob("key4", time.Now().Add(time.Millisecond*500))
	jh.Push(job4)

	assert.Equal(t, "key4", jh.Peek().Key)
	assert.Equal(t, 0, jh.heap.index["key4"])

	jh.Push(NewJob("key1", time.Now().Add(50*time.Millisecond)))

	assert.Equal(t, "key1", jh.Peek().Key)
	assert.Equal(t, 0, jh.heap.index["key1"])

	jh.Remove("key2")
	assert.Nil(t, jh.Get("key2"))
}
