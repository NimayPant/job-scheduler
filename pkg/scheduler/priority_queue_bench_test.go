package scheduler

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func BenchmarkEnqueue(b *testing.B) {
	pq := NewPriorityQueue()
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pq.EnqueueWithTime(
			fmt.Sprintf("task-%d", i),
			"job-1",
			i%10,
			now.Add(time.Duration(i)*time.Millisecond),
		)
	}
}

func BenchmarkDequeue(b *testing.B) {
	pq := NewPriorityQueue()
	now := time.Now()
	for i := 0; i < b.N; i++ {
		pq.EnqueueWithTime(
			fmt.Sprintf("task-%d", i),
			"job-1",
			i%10,
			now.Add(time.Duration(i)*time.Millisecond),
		)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pq.Dequeue()
	}
}

func BenchmarkEnqueueDequeue(b *testing.B) {
	pq := NewPriorityQueue()
	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pq.EnqueueWithTime(
			fmt.Sprintf("task-%d", i),
			"job-1",
			i%5,
			now.Add(time.Duration(i)*time.Millisecond),
		)
		pq.Dequeue()
	}
}

func BenchmarkPeek(b *testing.B) {
	pq := NewPriorityQueue()
	now := time.Now()
	for i := 0; i < 1000; i++ {
		pq.EnqueueWithTime(fmt.Sprintf("task-%d", i), "job-1", i%10, now)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pq.Peek()
	}
}

func BenchmarkDrainCycle(b *testing.B) {
	for _, size := range []int{100, 1000} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				pq := NewPriorityQueue()
				now := time.Now()
				for i := 0; i < size; i++ {
					pq.EnqueueWithTime(fmt.Sprintf("task-%d", i), "job-1", i%10, now)
				}
				pq.Drain()
			}
		})
	}
}

func BenchmarkRemoveCycle(b *testing.B) {
	for _, size := range []int{100, 1000} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				pq := NewPriorityQueue()
				now := time.Now()
				for i := 0; i < size; i++ {
					pq.EnqueueWithTime(fmt.Sprintf("task-%d", i), "job-1", i%10, now)
				}
				pq.Remove(fmt.Sprintf("task-%d", size/2))
			}
		})
	}
}

func BenchmarkConcurrentEnqueueDequeue(b *testing.B) {
	pq := NewPriorityQueue()
	now := time.Now()
	for i := 0; i < 1000; i++ {
		pq.EnqueueWithTime(fmt.Sprintf("seed-%d", i), "job-1", i%10, now)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				pq.EnqueueWithTime(
					fmt.Sprintf("task-%d", i),
					"job-1",
					i%10,
					now.Add(time.Duration(i)*time.Microsecond),
				)
			} else {
				pq.Dequeue()
			}
			i++
		}
	})
}

func BenchmarkHighContentionEnqueue(b *testing.B) {
	pq := NewPriorityQueue()
	now := time.Now()
	var mu sync.Mutex
	counter := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			id := counter
			counter++
			mu.Unlock()
			pq.EnqueueWithTime(fmt.Sprintf("task-%d", id), "job-1", id%10, now)
		}
	})
}
