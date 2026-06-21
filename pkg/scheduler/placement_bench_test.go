package scheduler

import (
	"fmt"
	"testing"

	"github.com/NimayPant/job-scheduler/pkg/models"
	"github.com/NimayPant/job-scheduler/pkg/store"
)

func makeWorkers(count int) []*store.WorkerRecord {
	workers := make([]*store.WorkerRecord, count)
	for i := range workers {
		total := models.ResourceRequirements{
			CPUCores: 16,
			MemoryMB: 32768,
			GPUs:     2,
			DiskMB:   102400,
		}
		cap := models.NewResourceCapacity(total)
		cap.Available.CPUCores = 16 - (i % 8)
		cap.Available.MemoryMB = 32768 - int64(i*1024)
		cap.Available.GPUs = 2 - (i % 2)

		workers[i] = &store.WorkerRecord{
			ID:        fmt.Sprintf("worker-%d", i),
			Address:   fmt.Sprintf("10.0.0.%d:9090", i+1),
			State:     models.WorkerStateActive,
			Resources: cap,
		}
	}
	return workers
}

func smallTask() *models.Task {
	return &models.Task{
		ID:   "t-small",
		Name: "small",
		ResourceReqs: models.ResourceRequirements{
			CPUCores: 1,
			MemoryMB: 512,
		},
	}
}

func gpuTask() *models.Task {
	return &models.Task{
		ID:   "t-gpu",
		Name: "gpu-train",
		ResourceReqs: models.ResourceRequirements{
			CPUCores: 4,
			MemoryMB: 8192,
			GPUs:     1,
			DiskMB:   20480,
		},
	}
}

func BenchmarkScoreWorkerBestFit(b *testing.B) {
	placer := NewPlacer(StrategyBestFit)
	workers := makeWorkers(1)
	task := smallTask()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		placer.ScoreWorker(task, workers[0])
	}
}

func BenchmarkScoreWorkerSpread(b *testing.B) {
	placer := NewPlacer(StrategySpread)
	workers := makeWorkers(1)
	task := smallTask()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		placer.ScoreWorker(task, workers[0])
	}
}

func BenchmarkSelectWorker(b *testing.B) {
	for _, workerCount := range []int{5, 20, 100, 500} {
		b.Run(fmt.Sprintf("workers-%d/bestfit", workerCount), func(b *testing.B) {
			placer := NewPlacer(StrategyBestFit)
			workers := makeWorkers(workerCount)
			task := gpuTask()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = placer.SelectWorker(task, workers)
			}
		})
		b.Run(fmt.Sprintf("workers-%d/spread", workerCount), func(b *testing.B) {
			placer := NewPlacer(StrategySpread)
			workers := makeWorkers(workerCount)
			task := gpuTask()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = placer.SelectWorker(task, workers)
			}
		})
	}
}


func BenchmarkEstimateClusterCapacity(b *testing.B) {
	for _, workerCount := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("workers-%d", workerCount), func(b *testing.B) {
			workers := makeWorkers(workerCount)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				EstimateClusterCapacity(workers)
			}
		})
	}
}

func BenchmarkCanFitAnywhere(b *testing.B) {
	for _, workerCount := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("workers-%d/fits", workerCount), func(b *testing.B) {
			workers := makeWorkers(workerCount)
			task := smallTask()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				CanFitAnywhere(task, workers)
			}
		})
		b.Run(fmt.Sprintf("workers-%d/no-fit", workerCount), func(b *testing.B) {
			workers := makeWorkers(workerCount)
			huge := &models.Task{
				ID: "t-huge",
				ResourceReqs: models.ResourceRequirements{
					CPUCores: 128,
					MemoryMB: 999999,
					GPUs:     16,
				},
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				CanFitAnywhere(huge, workers)
			}
		})
	}
}
