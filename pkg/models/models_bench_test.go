package models

import (
	"fmt"
	"testing"
)

func BenchmarkResourceFits(b *testing.B) {
	cap := NewResourceCapacity(ResourceRequirements{
		CPUCores: 32,
		MemoryMB: 65536,
		GPUs:     4,
		DiskMB:   204800,
	})
	req := ResourceRequirements{
		CPUCores: 4,
		MemoryMB: 8192,
		GPUs:     1,
		DiskMB:   20480,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cap.Fits(req)
	}
}

func BenchmarkResourceAllocate(b *testing.B) {
	req := ResourceRequirements{
		CPUCores: 1,
		MemoryMB: 512,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cap := NewResourceCapacity(ResourceRequirements{
			CPUCores: 32,
			MemoryMB: 65536,
		})
		_ = cap.Allocate(req)
	}
}

func BenchmarkResourceRelease(b *testing.B) {
	req := ResourceRequirements{
		CPUCores: 2,
		MemoryMB: 1024,
		GPUs:     1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cap := NewResourceCapacity(ResourceRequirements{
			CPUCores: 32,
			MemoryMB: 65536,
			GPUs:     4,
		})
		_ = cap.Allocate(req)
		cap.Release(req)
	}
}

func BenchmarkAllocateReleaseCycle(b *testing.B) {
	total := ResourceRequirements{
		CPUCores: 64,
		MemoryMB: 131072,
		GPUs:     8,
		DiskMB:   512000,
	}
	req := ResourceRequirements{
		CPUCores: 2,
		MemoryMB: 4096,
		GPUs:     1,
		DiskMB:   10240,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cap := NewResourceCapacity(total)
		for j := 0; j < 8; j++ {
			_ = cap.Allocate(req)
		}
		for j := 0; j < 8; j++ {
			cap.Release(req)
		}
	}
}

func BenchmarkUtilizationPercent(b *testing.B) {
	total := ResourceRequirements{
		CPUCores: 32,
		MemoryMB: 65536,
	}
	cap := NewResourceCapacity(total)
	_ = cap.Allocate(ResourceRequirements{CPUCores: 20, MemoryMB: 40000})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cap.UtilizationPercent()
	}
}

func BenchmarkJobValidation(b *testing.B) {
	for _, taskCount := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("tasks-%d", taskCount), func(b *testing.B) {
			job := &Job{
				Name:     "bench-job",
				Priority: 5,
			}
			for i := 0; i < taskCount; i++ {
				job.Tasks = append(job.Tasks, &Task{
					ID:      fmt.Sprintf("t-%d", i),
					Name:    fmt.Sprintf("task-%d", i),
					Command: "echo",
					Args:    []string{"hello"},
				})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = job.Validate()
			}
		})
	}
}

func BenchmarkTaskByID(b *testing.B) {
	for _, taskCount := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("tasks-%d", taskCount), func(b *testing.B) {
			job := &Job{Name: "bench", Priority: 1}
			for i := 0; i < taskCount; i++ {
				job.Tasks = append(job.Tasks, &Task{
					ID:      fmt.Sprintf("t-%d", i),
					Command: "true",
				})
			}
			target := fmt.Sprintf("t-%d", taskCount-1)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				job.TaskByID(target)
			}
		})
	}
}

func BenchmarkAllTasksTerminal(b *testing.B) {
	for _, taskCount := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("tasks-%d", taskCount), func(b *testing.B) {
			job := &Job{Name: "bench", Priority: 1}
			for i := 0; i < taskCount; i++ {
				job.Tasks = append(job.Tasks, &Task{
					ID:      fmt.Sprintf("t-%d", i),
					State:   TaskStateCompleted,
					Command: "true",
				})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				job.AllTasksTerminal()
			}
		})
	}
}
