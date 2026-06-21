package scheduler

import (
	"log"
	"sync"
	"time"

	"github.com/NimayPant/job-scheduler/pkg/models"
	"github.com/NimayPant/job-scheduler/pkg/raft"
	"github.com/NimayPant/job-scheduler/pkg/store"
)

const (
	schedulerTickInterval  = 100 * time.Millisecond
	heartbeatTimeout       = 30 * time.Second
	heartbeatCheckInterval = 10 * time.Second
)

type Scheduler struct {
	node           *raft.RaftNode
	queue          *PriorityQueue
	placer         *Placer
	dagExecutors   map[string]*DAGExecutor // dagID → executor
	taskDispatcher TaskDispatcher

	mu     sync.Mutex
	stopCh chan struct{}
}

type TaskDispatcher interface {
	DispatchTask(workerAddress string, task *models.Task) error
	CancelTaskOnWorker(workerAddress string, taskID string) error
}

func NewScheduler(node *raft.RaftNode, dispatcher TaskDispatcher) *Scheduler {
	return &Scheduler{
		node:           node,
		queue:          NewPriorityQueue(),
		placer:         NewPlacer(StrategyBestFit),
		dagExecutors:   make(map[string]*DAGExecutor),
		taskDispatcher: dispatcher,
		stopCh:         make(chan struct{}),
	}
}

func (s *Scheduler) Start() {
	go s.schedulingLoop()
	go s.healthCheckLoop()
	log.Println("scheduler started")
}

func (s *Scheduler) Stop() {
	close(s.stopCh)
	log.Println("scheduler stopped")
}

func (s *Scheduler) EnqueueJob(job *models.Job) {
	for _, task := range job.Tasks {
		if len(task.Dependencies) == 0 {
			s.queue.Enqueue(task.ID, job.ID, job.Priority)
		}
		// Tasks with dependencies will be enqueued by the DAG executor when ready.
	}
}

func (s *Scheduler) RegisterDAG(dag *models.DAG, job *models.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	executor := NewDAGExecutor(dag)
	s.dagExecutors[dag.ID] = executor

	for _, taskID := range executor.RootTasks() {
		s.queue.Enqueue(taskID, job.ID, job.Priority)
	}
}

func (s *Scheduler) schedulingLoop() {
	ticker := time.NewTicker(schedulerTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if !s.node.IsLeader() {
				continue
			}
			s.scheduleTick()
		}
	}
}

func (s *Scheduler) scheduleTick() {
	workers := s.node.FSM().GetActiveWorkers()
	if len(workers) == 0 {
		return
	}

	// Process up to 10 tasks per tick to avoid holding the lock too long.
	for i := 0; i < 10; i++ {
		item := s.queue.Peek()
		if item == nil {
			break
		}

		task, ok := s.node.FSM().GetTask(item.TaskID)
		if !ok {
			s.queue.Dequeue()
			continue
		}

		if task.State.IsTerminal() {
			s.queue.Dequeue()
			continue
		}

		worker, _, err := s.placer.SelectWorker(task, workers)
		if err != nil {
			break
		}

		s.queue.Dequeue()

		err = s.node.ApplyCommand(raft.CmdUpdateTaskState, raft.UpdateTaskStatePayload{
			TaskID: task.ID,
			JobID:  task.JobID,
			State:  models.TaskStateRunning,
			Worker: worker.ID,
		})
		if err != nil {
			log.Printf("failed to apply task assignment for %s: %v", task.ID, err)
			s.queue.Enqueue(task.ID, task.JobID, item.Priority)
			continue
		}

		if s.taskDispatcher != nil {
			if err := s.taskDispatcher.DispatchTask(worker.Address, task); err != nil {
				log.Printf("failed to dispatch task %s to worker %s: %v", task.ID, worker.ID, err)
				_ = s.node.ApplyCommand(raft.CmdUpdateTaskState, raft.UpdateTaskStatePayload{
					TaskID: task.ID,
					JobID:  task.JobID,
					State:  models.TaskStateQueued,
				})
				s.queue.Enqueue(task.ID, task.JobID, item.Priority)
			}
		}

		_ = worker.Resources.Allocate(task.ResourceReqs)
		worker.RunningTaskIDs = append(worker.RunningTaskIDs, task.ID)
	}
}

func (s *Scheduler) healthCheckLoop() {
	ticker := time.NewTicker(heartbeatCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if !s.node.IsLeader() {
				continue
			}
			s.checkWorkerHealth()
		}
	}
}

func (s *Scheduler) checkWorkerHealth() {
	workers := s.node.FSM().ListWorkers()
	now := time.Now()

	for _, w := range workers {
		if w.State != models.WorkerStateActive {
			continue
		}
		if now.Sub(w.LastHeartbeat) > heartbeatTimeout {
			log.Printf("worker %s missed heartbeat, marking as dead", w.ID)

			_ = s.node.ApplyCommand(raft.CmdRemoveWorker, raft.RemoveWorkerPayload{
				WorkerID: w.ID,
			})

			s.requeueWorkerTasks(w)
		}
	}
}

func (s *Scheduler) requeueWorkerTasks(worker *store.WorkerRecord) {
	for _, taskID := range worker.RunningTaskIDs {
		task, ok := s.node.FSM().GetTask(taskID)
		if !ok {
			continue
		}
		if task.State != models.TaskStateRunning {
			continue
		}

		if task.Attempt >= task.MaxRetries {
			log.Printf("task %s exhausted retries (%d/%d), marking failed", task.ID, task.Attempt, task.MaxRetries)
			_ = s.node.ApplyCommand(raft.CmdUpdateTaskState, raft.UpdateTaskStatePayload{
				TaskID:       task.ID,
				JobID:        task.JobID,
				State:        models.TaskStateFailed,
				ErrorMessage: "worker died, retries exhausted",
			})
			continue
		}

		log.Printf("requeuing task %s from dead worker %s (attempt %d)", task.ID, worker.ID, task.Attempt+1)
		_ = s.node.ApplyCommand(raft.CmdUpdateTaskState, raft.UpdateTaskStatePayload{
			TaskID: task.ID,
			JobID:  task.JobID,
			State:  models.TaskStateQueued,
		})

		if job, ok := s.node.FSM().GetJob(task.JobID); ok {
			s.queue.Enqueue(task.ID, task.JobID, job.Priority)
		}
	}
}

func (s *Scheduler) HandleTaskCompletion(taskID, jobID string, state models.TaskState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for dagID, executor := range s.dagExecutors {
		executor.UpdateTaskState(taskID, state)

		if state == models.TaskStateFailed {
			cancelled := executor.PropagateFailure(taskID)
			for _, cid := range cancelled {
				_ = s.node.ApplyCommand(raft.CmdUpdateTaskState, raft.UpdateTaskStatePayload{
					TaskID:       cid,
					JobID:        jobID,
					State:        models.TaskStateCancelled,
					ErrorMessage: "upstream dependency failed",
				})
			}
		} else if state == models.TaskStateCompleted {
			readyTasks := executor.GetReadyTasks()
			if job, ok := s.node.FSM().GetJob(jobID); ok {
				for _, readyID := range readyTasks {
					s.queue.Enqueue(readyID, jobID, job.Priority)
					executor.UpdateTaskState(readyID, models.TaskStateQueued)
				}
			}
		}

		if executor.IsComplete() {
			delete(s.dagExecutors, dagID)
		}
	}
}
