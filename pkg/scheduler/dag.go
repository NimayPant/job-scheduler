package scheduler

import (
	"fmt"

	"github.com/NimayPant/job-scheduler/pkg/models"
)

type DAGExecutor struct {
	dag        *models.DAG
	taskMap    map[string]*models.Task      // taskID → Task
	children   map[string][]string          // taskID → downstream task IDs
	parents    map[string][]string          // taskID → upstream task IDs
	taskStates map[string]models.TaskState  // taskID → current state
}

func NewDAGExecutor(dag *models.DAG) *DAGExecutor {
	e := &DAGExecutor{
		dag:        dag,
		taskMap:    make(map[string]*models.Task),
		children:   make(map[string][]string),
		parents:    make(map[string][]string),
		taskStates: make(map[string]models.TaskState),
	}
	for _, t := range dag.Tasks {
		e.taskMap[t.ID] = t
		e.taskStates[t.ID] = t.State
	}
	for _, edge := range dag.Edges {
		e.children[edge.FromTask] = append(e.children[edge.FromTask], edge.ToTask)
		e.parents[edge.ToTask] = append(e.parents[edge.ToTask], edge.FromTask)
	}
	return e
}

// Validate checks for cycles and unknown task references
func ValidateDAG(dag *models.DAG) error {
	taskSet := make(map[string]bool)
	for _, t := range dag.Tasks {
		if taskSet[t.ID] {
			return fmt.Errorf("duplicate task ID: %s", t.ID)
		}
		taskSet[t.ID] = true
	}

	for _, edge := range dag.Edges {
		if !taskSet[edge.FromTask] {
			return fmt.Errorf("edge references unknown task: %s", edge.FromTask)
		}
		if !taskSet[edge.ToTask] {
			return fmt.Errorf("edge references unknown task: %s", edge.ToTask)
		}
		if edge.FromTask == edge.ToTask {
			return fmt.Errorf("self-loop detected on task: %s", edge.FromTask)
		}
	}

	adj := make(map[string][]string)
	for _, edge := range dag.Edges {
		adj[edge.FromTask] = append(adj[edge.FromTask], edge.ToTask)
	}

	// 3-color DFS: 0=white, 1=gray, 2=black
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int)
	for id := range taskSet {
		color[id] = white
	}

	var dfs func(node string) error
	dfs = func(node string) error {
		color[node] = gray
		for _, neighbor := range adj[node] {
			if color[neighbor] == gray {
				return fmt.Errorf("cycle detected involving task: %s → %s", node, neighbor)
			}
			if color[neighbor] == white {
				if err := dfs(neighbor); err != nil {
					return err
				}
			}
		}
		color[node] = black
		return nil
	}

	for id := range taskSet {
		if color[id] == white {
			if err := dfs(id); err != nil {
				return err
			}
		}
	}

	return nil
}

// Kahn's algorithm
func TopologicalSort(dag *models.DAG) ([]string, error) {
	inDegree := make(map[string]int)
	adj := make(map[string][]string)
	for _, t := range dag.Tasks {
		inDegree[t.ID] = 0
	}
	for _, edge := range dag.Edges {
		adj[edge.FromTask] = append(adj[edge.FromTask], edge.ToTask)
		inDegree[edge.ToTask]++
	}

	queue := make([]string, 0)
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	var sorted []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		sorted = append(sorted, node)

		for _, neighbor := range adj[node] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(sorted) != len(dag.Tasks) {
		return nil, fmt.Errorf("DAG contains a cycle: sorted %d of %d tasks", len(sorted), len(dag.Tasks))
	}
	return sorted, nil
}

func (e *DAGExecutor) GetReadyTasks() []string {
	var ready []string
	for taskID, state := range e.taskStates {
		if state != models.TaskStatePending {
			continue
		}
		allDepsComplete := true
		for _, parentID := range e.parents[taskID] {
			if e.taskStates[parentID] != models.TaskStateCompleted {
				allDepsComplete = false
				break
			}
		}
		if allDepsComplete {
			ready = append(ready, taskID)
		}
	}
	return ready
}

func (e *DAGExecutor) UpdateTaskState(taskID string, state models.TaskState) {
	e.taskStates[taskID] = state
}

func (e *DAGExecutor) PropagateFailure(failedTaskID string) []string {
	var cancelled []string
	visited := make(map[string]bool)

	var propagate func(taskID string)
	propagate = func(taskID string) {
		for _, childID := range e.children[taskID] {
			if visited[childID] {
				continue
			}
			visited[childID] = true
			e.taskStates[childID] = models.TaskStateCancelled
			cancelled = append(cancelled, childID)
			propagate(childID)
		}
	}

	propagate(failedTaskID)
	return cancelled
}

func (e *DAGExecutor) IsComplete() bool {
	for _, state := range e.taskStates {
		if !state.IsTerminal() {
			return false
		}
	}
	return true
}

func (e *DAGExecutor) HasFailures() bool {
	for _, state := range e.taskStates {
		if state == models.TaskStateFailed {
			return true
		}
	}
	return false
}

func (e *DAGExecutor) RootTasks() []string {
	var roots []string
	for taskID := range e.taskMap {
		if len(e.parents[taskID]) == 0 {
			roots = append(roots, taskID)
		}
	}
	return roots
}
