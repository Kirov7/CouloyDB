package ds

import (
	"container/heap"
	"time"
)

type TimeHeap struct {
	heap h
}

func NewTimeHeap() *TimeHeap {
	return &TimeHeap{
		heap: h{
			heap:  make([]*Job, 0),
			index: make(map[string]int),
		},
	}
}

func (jh *TimeHeap) Push(job *Job) {
	if _, ok := jh.heap.index[job.Key]; ok {
		jh.heap.update(job)
	} else {
		heap.Push(&jh.heap, job)
	}
}

func (jh *TimeHeap) Pop() *Job {
	if jh.heap.isEmpty() {
		return nil
	}
	return heap.Pop(&jh.heap).(*Job)
}

func (jh *TimeHeap) Get(key string) *Job {
	if i, ok := jh.heap.index[key]; ok {
		return jh.heap.heap[i]
	}
	return nil
}

func (jh *TimeHeap) Remove(key string) {
	if i, ok := jh.heap.index[key]; ok {
		delete(jh.heap.index, key)
		heap.Remove(&jh.heap, i)
	}
}

func (jh *TimeHeap) IsExpired(key string) bool {
	if _, ok := jh.heap.index[key]; ok {
		return jh.Get(key).Expiration.Before(time.Now())
	}
	return true
}

func (jh *TimeHeap) Peek() *Job {
	if jh.IsEmpty() {
		return nil
	}
	return jh.heap.peek().(*Job)
}

func (jh *TimeHeap) IsEmpty() bool {
	return jh.heap.isEmpty()
}

type Job struct {
	Key        string
	Expiration time.Time
}

func NewJob(key string, expiration time.Time) *Job {
	return &Job{
		Key:        key,
		Expiration: expiration,
	}
}

type h struct {
	heap  []*Job
	index map[string]int
}

// Push adds a job to the heap.
func (h *h) Push(j interface{}) {
	job := j.(*Job)
	h.heap = append(h.heap, job)
	h.index[job.Key] = len(h.heap) - 1
}

// Pop removes and returns the job with the earliest expiration time from the heap.
func (h *h) Pop() interface{} {
	if h.isEmpty() {
		return nil
	}
	old := h.heap
	n := len(old)
	x := old[n-1]
	h.heap = old[0 : n-1]
	delete(h.index, x.Key)
	return x
}

// peek returns the job with the earliest expiration time without removing it from the heap.
func (h *h) peek() interface{} {
	if h.isEmpty() {
		return nil
	}
	return h.heap[0]
}

// Len returns the number of jobs in the heap.
func (h *h) Len() int {
	return len(h.heap)
}

// Less reports whether the job with index i should sort before the job with index j.
func (h *h) Less(i, j int) bool {
	return h.heap[i].Expiration.Before(h.heap[j].Expiration)
}

// Swap swaps the jobs with indexes i and j.
func (h *h) Swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
	h.index[h.heap[i].Key], h.index[h.heap[j].Key] = i, j
}

// isEmpty checks if the heap is empty.
func (h *h) isEmpty() bool {
	return len(h.heap) == 0
}

// update modifies the expiration time of a job in the heap.
func (h *h) update(job *Job) {
	if index, ok := h.index[job.Key]; ok {
		j := h.heap[index]
		j.Expiration = job.Expiration
		heap.Fix(h, index)
	}
}
