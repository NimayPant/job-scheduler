package worker

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/NimayPant/job-scheduler/pkg/models"
)

const (
	heartbeatInterval = 10 * time.Second
)

type CoordinationClient interface {
	Register(ctx context.Context, workerID, address string, resources models.ResourceCapacity) error
	SendHeartbeat(ctx context.Context, workerID string, resources models.ResourceCapacity, runningTaskIDs []string) error
	ReportTaskStatus(ctx context.Context, workerID, taskID, jobID string, state models.TaskState, exitCode int, stdout, stderr, errMsg string) error
}

type Worker struct {
	ID        string
	Address   string
	Resources models.ResourceCapacity
	client    CoordinationClient

	mu           sync.Mutex
	runningTasks map[string]*runningTask
	executor     *Executor

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type runningTask struct {
	task   *models.Task
	cancel context.CancelFunc
}

func NewWorker(id, address string, resources models.ResourceCapacity, client CoordinationClient) *Worker {
	return &Worker{
		ID:           id,
		Address:      address,
		Resources:    resources,
		client:       client,
		runningTasks: make(map[string]*runningTask),
		executor:     NewExecutor(),
		stopCh:       make(chan struct{}),
	}
}

func DetectResources() models.ResourceCapacity {
	cpus := runtime.NumCPU()
	// Estimate 1GB per core
	memMB := int64(cpus) * 1024
	total := models.ResourceRequirements{
		CPUCores: cpus,
		MemoryMB: memMB,
		GPUs:     0,
		DiskMB:   10240, // 10GB default
	}
	return models.NewResourceCapacity(total)
}

func (w *Worker) Start(ctx context.Context) error {
	log.Printf("worker %s starting at %s", w.ID, w.Address)

	if w.client != nil {
		if err := w.client.Register(ctx, w.ID, w.Address, w.Resources); err != nil {
			return fmt.Errorf("failed to register worker: %w", err)
		}
		log.Printf("worker %s registered with scheduler cluster", w.ID)
	}

	w.wg.Add(1)
	go w.heartbeatLoop()

	return nil
}

func (w *Worker) Stop() {
	log.Printf("worker %s shutting down...", w.ID)
	close(w.stopCh)

	// Cancel all running tasks.
	w.mu.Lock()
	for _, rt := range w.runningTasks {
		rt.cancel()
	}
	w.mu.Unlock()

	w.wg.Wait()
	log.Printf("worker %s shutdown complete", w.ID)
}

func (w *Worker) AssignTask(task *models.Task) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.runningTasks[task.ID]; exists {
		return fmt.Errorf("task %s already running", task.ID)
	}

	if !w.Resources.Fits(task.ResourceReqs) {
		return fmt.Errorf("insufficient resources for task %s: need %s", task.ID, task.ResourceReqs)
	}

	if err := w.Resources.Allocate(task.ResourceReqs); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.runningTasks[task.ID] = &runningTask{
		task:   task,
		cancel: cancel,
	}

	w.wg.Add(1)
	go w.executeTask(ctx, task)

	log.Printf("worker %s accepted task %s (%s)", w.ID, task.ID, task.Name)
	return nil
}

func (w *Worker) CancelTask(taskID string) error {
	w.mu.Lock()
	rt, exists := w.runningTasks[taskID]
	w.mu.Unlock()

	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	rt.cancel()
	log.Printf("worker %s cancelled task %s", w.ID, taskID)
	return nil
}

func (w *Worker) executeTask(ctx context.Context, task *models.Task) {
	defer w.wg.Done()

	result := w.executor.Execute(ctx, task)

	w.mu.Lock()
	delete(w.runningTasks, task.ID)
	w.Resources.Release(task.ResourceReqs)
	w.mu.Unlock()

	state := models.TaskStateCompleted
	if result.Error != "" {
		state = models.TaskStateFailed
	}

	if w.client != nil {
		if err := w.client.ReportTaskStatus(
			context.Background(),
			w.ID, task.ID, task.JobID,
			state, result.ExitCode,
			result.Stdout, result.Stderr, result.Error,
		); err != nil {
			log.Printf("[WORKER %s] failed to report task %s status: %v", w.ID, task.ID, err)
		}
	}

	log.Printf("worker %s task %s completed (exit=%d)", w.ID, task.ID, result.ExitCode)
}

func (w *Worker) heartbeatLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.sendHeartbeat()
		}
	}
}

func (w *Worker) sendHeartbeat() {
	w.mu.Lock()
	taskIDs := make([]string, 0, len(w.runningTasks))
	for id := range w.runningTasks {
		taskIDs = append(taskIDs, id)
	}
	resources := w.Resources
	w.mu.Unlock()

	if w.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := w.client.SendHeartbeat(ctx, w.ID, resources, taskIDs); err != nil {
			log.Printf("[WORKER %s] heartbeat failed: %v", w.ID, err)
		}
	}
}

func (w *Worker) RunningTaskCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.runningTasks)
}
