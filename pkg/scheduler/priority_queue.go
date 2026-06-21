package scheduler

import (
	"container/heap"
	"sync"
	"time"
)

type QueueItem struct {
	TaskID      string
	JobID       string
	Priority    int // Lower number = higher priority
	EnqueueTime time.Time
	index       int // Managed by heap.Interface
}

type innerHeap []*QueueItem

func (h innerHeap) Len() int { return len(h) }

func (h innerHeap) Less(i, j int) bool {
	// Primary sort: lower priority number = higher priority.
	if h[i].Priority != h[j].Priority {
		return h[i].Priority < h[j].Priority
	}
	// Tie-break: earlier enqueue time wins (FIFO within same priority).
	return h[i].EnqueueTime.Before(h[j].EnqueueTime)
}

func (h innerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *innerHeap) Push(x interface{}) {
	item := x.(*QueueItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *innerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

type PriorityQueue struct {
	mu   sync.Mutex
	heap innerHeap
}

func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		heap: make(innerHeap, 0),
	}
	heap.Init(&pq.heap)
	return pq
}

func (pq *PriorityQueue) Enqueue(taskID, jobID string, priority int) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(&pq.heap, &QueueItem{
		TaskID:      taskID,
		JobID:       jobID,
		Priority:    priority,
		EnqueueTime: time.Now(),
	})
}

func (pq *PriorityQueue) EnqueueWithTime(taskID, jobID string, priority int, t time.Time) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(&pq.heap, &QueueItem{
		TaskID:      taskID,
		JobID:       jobID,
		Priority:    priority,
		EnqueueTime: t,
	})
}

func (pq *PriorityQueue) Dequeue() *QueueItem {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if pq.heap.Len() == 0 {
		return nil
	}
	return heap.Pop(&pq.heap).(*QueueItem)
}

func (pq *PriorityQueue) Peek() *QueueItem {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if pq.heap.Len() == 0 {
		return nil
	}
	return pq.heap[0]
}

func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.heap.Len()
}

func (pq *PriorityQueue) Remove(taskID string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	for i, item := range pq.heap {
		if item.TaskID == taskID {
			heap.Remove(&pq.heap, i)
			return true
		}
	}
	return false
}

func (pq *PriorityQueue) Drain() []*QueueItem {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	items := make([]*QueueItem, 0, pq.heap.Len())
	for pq.heap.Len() > 0 {
		items = append(items, heap.Pop(&pq.heap).(*QueueItem))
	}
	return items
}
