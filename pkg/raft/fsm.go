package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/NimayPant/job-scheduler/pkg/models"
	"github.com/NimayPant/job-scheduler/pkg/store"
	hraft "github.com/hashicorp/raft"
)

type CommandType uint8

const (
	CmdSubmitJob        CommandType = iota
	CmdUpdateTaskState
	CmdRegisterWorker
	CmdUpdateWorker
	CmdRemoveWorker
	CmdCancelJob
	CmdSubmitDAG
)

type Command struct {
	Type    CommandType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type SubmitJobPayload struct {
	Job *models.Job `json:"job"`
}

type UpdateTaskStatePayload struct {
	TaskID       string           `json:"task_id"`
	JobID        string           `json:"job_id"`
	State        models.TaskState `json:"state"`
	Worker       string           `json:"worker,omitempty"`
	ExitCode     int              `json:"exit_code,omitempty"`
	Stdout       string           `json:"stdout,omitempty"`
	Stderr       string           `json:"stderr,omitempty"`
	ErrorMessage string           `json:"error_message,omitempty"`
	Timestamp    time.Time        `json:"timestamp"`
}

type RegisterWorkerPayload struct {
	Worker *store.WorkerRecord `json:"worker"`
}

type UpdateWorkerPayload struct {
	WorkerID      string                  `json:"worker_id"`
	Resources     models.ResourceCapacity `json:"resources"`
	RunningTasks  []string                `json:"running_tasks"`
	LastHeartbeat time.Time               `json:"last_heartbeat"`
}

type RemoveWorkerPayload struct {
	WorkerID string `json:"worker_id"`
}

type CancelJobPayload struct {
	JobID     string    `json:"job_id"`
	Timestamp time.Time `json:"timestamp"`
}

type SubmitDAGPayload struct {
	DAG *models.DAG `json:"dag"`
	Job *models.Job `json:"job"`
}

type FSM struct {
	mu      sync.RWMutex
	jobs    map[string]*models.Job
	tasks   map[string]*models.Task
	workers map[string]*store.WorkerRecord
	dags    map[string]*models.DAG

	store *store.BoltStore
}

func NewFSM(s *store.BoltStore) *FSM {
	return &FSM{
		jobs:    make(map[string]*models.Job),
		tasks:   make(map[string]*models.Task),
		workers: make(map[string]*store.WorkerRecord),
		dags:    make(map[string]*models.DAG),
		store:   s,
	}
}

func (f *FSM) Apply(l *hraft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		log.Printf("[FSM] failed to unmarshal command: %v", err)
		return err
	}

	switch cmd.Type {
	case CmdSubmitJob:
		return f.applySubmitJob(cmd.Payload)
	case CmdUpdateTaskState:
		return f.applyUpdateTaskState(cmd.Payload)
	case CmdRegisterWorker:
		return f.applyRegisterWorker(cmd.Payload)
	case CmdUpdateWorker:
		return f.applyUpdateWorker(cmd.Payload)
	case CmdRemoveWorker:
		return f.applyRemoveWorker(cmd.Payload)
	case CmdCancelJob:
		return f.applyCancelJob(cmd.Payload)
	case CmdSubmitDAG:
		return f.applySubmitDAG(cmd.Payload)
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (f *FSM) applySubmitJob(data json.RawMessage) interface{} {
	var p SubmitJobPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	f.jobs[p.Job.ID] = p.Job
	for _, t := range p.Job.Tasks {
		f.tasks[t.ID] = t
	}

	if f.store != nil {
		if err := f.store.SaveJob(p.Job); err != nil {
			log.Printf("[FSM] failed to persist job %s: %v", p.Job.ID, err)
		}
	}
	return nil
}

func (f *FSM) applyUpdateTaskState(data json.RawMessage) interface{} {
	var p UpdateTaskStatePayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	task, ok := f.tasks[p.TaskID]
	if !ok {
		return fmt.Errorf("task %s not found", p.TaskID)
	}

	task.State = p.State
	task.UpdatedAt = p.Timestamp
	if p.Worker != "" {
		task.AssignedWorker = p.Worker
	}
	if p.State == models.TaskStateRunning {
		task.Attempt++
	}
	if p.State == models.TaskStateCompleted || p.State == models.TaskStateFailed {
		task.ExitCode = p.ExitCode
		task.Stdout = p.Stdout
		task.Stderr = p.Stderr
		task.ErrorMessage = p.ErrorMessage
	}

	if job, ok := f.jobs[p.JobID]; ok {
		f.updateJobState(job, p.Timestamp)
		if f.store != nil {
			if err := f.store.SaveJob(job); err != nil {
				log.Printf("[FSM] failed to persist job %s: %v", job.ID, err)
			}
		}
	}
	return nil
}

func (f *FSM) updateJobState(job *models.Job, timestamp time.Time) {
	allComplete := true
	anyRunning := false
	anyFailed := false

	for _, t := range job.Tasks {
		switch t.State {
		case models.TaskStateRunning, models.TaskStateRetrying:
			anyRunning = true
			allComplete = false
		case models.TaskStateFailed:
			anyFailed = true
		case models.TaskStateCompleted, models.TaskStateCancelled:
			// Terminal, continue.
		default:
			allComplete = false
		}
	}

	if allComplete && !anyFailed {
		job.State = models.JobStateCompleted
	} else if allComplete && anyFailed {
		job.State = models.JobStateFailed
	} else if anyRunning {
		job.State = models.JobStateRunning
	}
	job.UpdatedAt = timestamp
}

func (f *FSM) applyRegisterWorker(data json.RawMessage) interface{} {
	var p RegisterWorkerPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	f.workers[p.Worker.ID] = p.Worker
	if f.store != nil {
		if err := f.store.SaveWorker(p.Worker); err != nil {
			log.Printf("[FSM] failed to persist worker %s: %v", p.Worker.ID, err)
		}
	}
	return nil
}

func (f *FSM) applyUpdateWorker(data json.RawMessage) interface{} {
	var p UpdateWorkerPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	w, ok := f.workers[p.WorkerID]
	if !ok {
		return fmt.Errorf("worker %s not found", p.WorkerID)
	}
	w.Resources = p.Resources
	w.RunningTaskIDs = p.RunningTasks
	w.LastHeartbeat = p.LastHeartbeat

	if f.store != nil {
		if err := f.store.SaveWorker(w); err != nil {
			log.Printf("[FSM] failed to persist worker %s: %v", w.ID, err)
		}
	}
	return nil
}

func (f *FSM) applyRemoveWorker(data json.RawMessage) interface{} {
	var p RemoveWorkerPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if w, ok := f.workers[p.WorkerID]; ok {
		w.State = models.WorkerStateDead
	}
	if f.store != nil {
		_ = f.store.DeleteWorker(p.WorkerID)
	}
	return nil
}

func (f *FSM) applyCancelJob(data json.RawMessage) interface{} {
	var p CancelJobPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	job, ok := f.jobs[p.JobID]
	if !ok {
		return fmt.Errorf("job %s not found", p.JobID)
	}
	job.State = models.JobStateCancelled
	job.UpdatedAt = p.Timestamp
	for _, t := range job.Tasks {
		if !t.State.IsTerminal() {
			t.State = models.TaskStateCancelled
			t.UpdatedAt = p.Timestamp
		}
	}
	if f.store != nil {
		_ = f.store.SaveJob(job)
	}
	return nil
}

func (f *FSM) applySubmitDAG(data json.RawMessage) interface{} {
	var p SubmitDAGPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	f.dags[p.DAG.ID] = p.DAG
	f.jobs[p.Job.ID] = p.Job
	for _, t := range p.Job.Tasks {
		f.tasks[t.ID] = t
	}

	if f.store != nil {
		_ = f.store.SaveDAG(p.DAG)
		_ = f.store.SaveJob(p.Job)
	}
	return nil
}

func (f *FSM) GetJob(id string) (*models.Job, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	j, ok := f.jobs[id]
	return j, ok
}

func (f *FSM) GetTask(id string) (*models.Task, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	t, ok := f.tasks[id]
	return t, ok
}

func (f *FSM) GetWorker(id string) (*store.WorkerRecord, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	w, ok := f.workers[id]
	return w, ok
}

func (f *FSM) ListJobs() []*models.Job {
	f.mu.RLock()
	defer f.mu.RUnlock()
	jobs := make([]*models.Job, 0, len(f.jobs))
	for _, j := range f.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (f *FSM) ListWorkers() []*store.WorkerRecord {
	f.mu.RLock()
	defer f.mu.RUnlock()
	workers := make([]*store.WorkerRecord, 0, len(f.workers))
	for _, w := range f.workers {
		workers = append(workers, w)
	}
	return workers
}

func (f *FSM) GetActiveWorkers() []*store.WorkerRecord {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var active []*store.WorkerRecord
	for _, w := range f.workers {
		if w.State == models.WorkerStateActive {
			active = append(active, w)
		}
	}
	return active
}

func (f *FSM) GetPendingTasks() []*models.Task {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var pending []*models.Task
	for _, t := range f.tasks {
		if t.State == models.TaskStatePending || t.State == models.TaskStateQueued {
			pending = append(pending, t)
		}
	}
	return pending
}

func (f *FSM) GetDAG(id string) (*models.DAG, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	d, ok := f.dags[id]
	return d, ok
}

func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	state := &fsmState{
		Jobs:    make(map[string]*models.Job, len(f.jobs)),
		Tasks:   make(map[string]*models.Task, len(f.tasks)),
		Workers: make(map[string]*store.WorkerRecord, len(f.workers)),
		DAGs:    make(map[string]*models.DAG, len(f.dags)),
	}
	for k, v := range f.jobs {
		state.Jobs[k] = v
	}
	for k, v := range f.tasks {
		state.Tasks[k] = v
	}
	for k, v := range f.workers {
		state.Workers[k] = v
	}
	for k, v := range f.dags {
		state.DAGs[k] = v
	}

	return &FSMSnapshot{state: state}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var state fsmState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.jobs = state.Jobs
	f.tasks = state.Tasks
	f.workers = state.Workers
	f.dags = state.DAGs

	if f.store != nil {
		for _, j := range f.jobs {
			_ = f.store.SaveJob(j)
		}
		for _, w := range f.workers {
			_ = f.store.SaveWorker(w)
		}
		for _, d := range f.dags {
			_ = f.store.SaveDAG(d)
		}
	}

	return nil
}

type fsmState struct {
	Jobs    map[string]*models.Job         `json:"jobs"`
	Tasks   map[string]*models.Task        `json:"tasks"`
	Workers map[string]*store.WorkerRecord `json:"workers"`
	DAGs    map[string]*models.DAG         `json:"dags"`
}
