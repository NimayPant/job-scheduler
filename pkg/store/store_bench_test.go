package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NimayPant/job-scheduler/pkg/models"
)

func tempStore(b *testing.B) (*BoltStore, func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "boltbench-*")
	if err != nil {
		b.Fatal(err)
	}
	s, err := NewBoltStore(filepath.Join(dir, "bench.db"))
	if err != nil {
		os.RemoveAll(dir)
		b.Fatal(err)
	}
	return s, func() {
		s.Close()
		os.RemoveAll(dir)
	}
}

func makeJob(id string, taskCount int) *models.Job {
	job := &models.Job{
		ID:       id,
		Name:     "bench-job-" + id,
		Priority: 5,
		State:    models.JobStatePending,
	}
	for i := 0; i < taskCount; i++ {
		job.Tasks = append(job.Tasks, &models.Task{
			ID:      fmt.Sprintf("%s-task-%d", id, i),
			JobID:   id,
			Name:    fmt.Sprintf("task-%d", i),
			Command: "echo",
			Args:    []string{"hello"},
			State:   models.TaskStatePending,
		})
	}
	return job
}

func BenchmarkSaveJob(b *testing.B) {
	for _, taskCount := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("tasks-%d", taskCount), func(b *testing.B) {
			s, cleanup := tempStore(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				job := makeJob(fmt.Sprintf("job-%d", i), taskCount)
				if err := s.SaveJob(job); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGetJob(b *testing.B) {
	s, cleanup := tempStore(b)
	defer cleanup()

	job := makeJob("get-bench", 10)
	if err := s.SaveJob(job); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.GetJob("get-bench"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListJobs(b *testing.B) {
	for _, jobCount := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("jobs-%d", jobCount), func(b *testing.B) {
			s, cleanup := tempStore(b)
			defer cleanup()

			for i := 0; i < jobCount; i++ {
				s.SaveJob(makeJob(fmt.Sprintf("job-%d", i), 5))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.ListJobs(nil, 0)
			}
		})
	}
}

func BenchmarkGetTask(b *testing.B) {
	s, cleanup := tempStore(b)
	defer cleanup()

	job := makeJob("task-bench", 20)
	s.SaveJob(job)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.GetTask("task-bench-task-10")
	}
}

func BenchmarkUpdateTask(b *testing.B) {
	s, cleanup := tempStore(b)
	defer cleanup()

	job := makeJob("update-bench", 1)
	s.SaveJob(job)

	task := job.Tasks[0]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task.State = models.TaskState(i % 4)
		task.UpdatedAt = time.Now()
		if err := s.UpdateTask(task); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSaveWorker(b *testing.B) {
	s, cleanup := tempStore(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := &WorkerRecord{
			ID:      fmt.Sprintf("worker-%d", i),
			Address: fmt.Sprintf("10.0.0.%d:9090", i%255),
			State:   models.WorkerStateActive,
			Resources: models.NewResourceCapacity(models.ResourceRequirements{
				CPUCores: 16,
				MemoryMB: 32768,
			}),
			LastHeartbeat: time.Now(),
		}
		if err := s.SaveWorker(w); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListWorkers(b *testing.B) {
	for _, count := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("workers-%d", count), func(b *testing.B) {
			s, cleanup := tempStore(b)
			defer cleanup()

			for i := 0; i < count; i++ {
				s.SaveWorker(&WorkerRecord{
					ID:      fmt.Sprintf("w-%d", i),
					Address: fmt.Sprintf("10.0.0.%d:9090", i%255),
					State:   models.WorkerStateActive,
					Resources: models.NewResourceCapacity(models.ResourceRequirements{
						CPUCores: 8,
						MemoryMB: 16384,
					}),
					LastHeartbeat: time.Now(),
				})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.ListWorkers()
			}
		})
	}
}

func BenchmarkDeleteJob(b *testing.B) {
	s, cleanup := tempStore(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("del-%d", i)
		s.SaveJob(makeJob(id, 5))
		s.DeleteJob(id)
	}
}

func BenchmarkSaveDAG(b *testing.B) {
	s, cleanup := tempStore(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dag := &models.DAG{
			ID:       fmt.Sprintf("dag-%d", i),
			Name:     "bench-dag",
			Priority: 3,
		}
		for j := 0; j < 20; j++ {
			dag.Tasks = append(dag.Tasks, &models.Task{
				ID:      fmt.Sprintf("dag-%d-t-%d", i, j),
				Command: "echo",
			})
		}
		for j := 0; j < 19; j++ {
			dag.Edges = append(dag.Edges, models.DAGEdge{
				FromTask: fmt.Sprintf("dag-%d-t-%d", i, j),
				ToTask:   fmt.Sprintf("dag-%d-t-%d", i, j+1),
			})
		}
		if err := s.SaveDAG(dag); err != nil {
			b.Fatal(err)
		}
	}
}
