package scheduler

import (
	"fmt"
	"testing"

	"github.com/NimayPant/job-scheduler/pkg/models"
)

func buildDAG(taskCount int, edgeStyle string) *models.DAG {
	dag := &models.DAG{
		ID:   "dag-bench",
		Name: "benchmark-dag",
	}

	for i := 0; i < taskCount; i++ {
		dag.Tasks = append(dag.Tasks, &models.Task{
			ID:    fmt.Sprintf("t-%d", i),
			Name:  fmt.Sprintf("task-%d", i),
			State: models.TaskStatePending,
		})
	}

	switch edgeStyle {
	case "chain":
		for i := 0; i < taskCount-1; i++ {
			dag.Edges = append(dag.Edges, models.DAGEdge{
				FromTask: fmt.Sprintf("t-%d", i),
				ToTask:   fmt.Sprintf("t-%d", i+1),
			})
		}
	case "fan-out":
		for i := 1; i < taskCount; i++ {
			dag.Edges = append(dag.Edges, models.DAGEdge{
				FromTask: "t-0",
				ToTask:   fmt.Sprintf("t-%d", i),
			})
		}
	case "diamond":
		mid := taskCount / 2
		for i := 1; i <= mid; i++ {
			dag.Edges = append(dag.Edges, models.DAGEdge{
				FromTask: "t-0",
				ToTask:   fmt.Sprintf("t-%d", i),
			})
		}
		last := fmt.Sprintf("t-%d", taskCount-1)
		for i := 1; i <= mid; i++ {
			dag.Edges = append(dag.Edges, models.DAGEdge{
				FromTask: fmt.Sprintf("t-%d", i),
				ToTask:   last,
			})
		}
	}

	return dag
}

func BenchmarkValidateDAG(b *testing.B) {
	for _, tc := range []struct {
		tasks int
		style string
	}{
		{50, "chain"},
		{200, "chain"},
		{50, "fan-out"},
		{200, "fan-out"},
		{50, "diamond"},
	} {
		dag := buildDAG(tc.tasks, tc.style)
		b.Run(fmt.Sprintf("%s-%d", tc.style, tc.tasks), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = ValidateDAG(dag)
			}
		})
	}
}

func BenchmarkTopologicalSort(b *testing.B) {
	for _, tc := range []struct {
		tasks int
		style string
	}{
		{50, "chain"},
		{200, "chain"},
		{1000, "chain"},
		{50, "fan-out"},
		{200, "fan-out"},
	} {
		dag := buildDAG(tc.tasks, tc.style)
		b.Run(fmt.Sprintf("%s-%d", tc.style, tc.tasks), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = TopologicalSort(dag)
			}
		})
	}
}

func BenchmarkGetReadyTasks(b *testing.B) {
	for _, tc := range []struct {
		tasks int
		style string
	}{
		{50, "chain"},
		{200, "chain"},
		{50, "fan-out"},
		{200, "fan-out"},
	} {
		dag := buildDAG(tc.tasks, tc.style)
		exec := NewDAGExecutor(dag)
		b.Run(fmt.Sprintf("%s-%d", tc.style, tc.tasks), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				exec.GetReadyTasks()
			}
		})
	}
}

func BenchmarkPropagateFailure(b *testing.B) {
	for _, tc := range []struct {
		tasks int
		style string
	}{
		{50, "chain"},
		{200, "chain"},
		{50, "fan-out"},
	} {
		dag := buildDAG(tc.tasks, tc.style)
		b.Run(fmt.Sprintf("%s-%d", tc.style, tc.tasks), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				exec := NewDAGExecutor(dag)
				exec.UpdateTaskState("t-0", models.TaskStateFailed)
				exec.PropagateFailure("t-0")
			}
		})
	}
}

func BenchmarkDAGExecutorWalkthrough(b *testing.B) {
	dag := buildDAG(100, "chain")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec := NewDAGExecutor(dag)
		for taskIdx := 0; taskIdx < 100; taskIdx++ {
			ready := exec.GetReadyTasks()
			for _, tid := range ready {
				exec.UpdateTaskState(tid, models.TaskStateRunning)
			}
			for _, tid := range ready {
				exec.UpdateTaskState(tid, models.TaskStateCompleted)
			}
		}
	}
}
