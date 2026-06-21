package models

import (
	"fmt"
	"time"
)

type TaskState int

const (
	TaskStatePending TaskState = iota
	TaskStateQueued
	TaskStateRunning
	TaskStateCompleted
	TaskStateFailed
	TaskStateCancelled
	TaskStateRetrying
)

func (s TaskState) String() string {
	switch s {
	case TaskStatePending:
		return "PENDING"
	case TaskStateQueued:
		return "QUEUED"
	case TaskStateRunning:
		return "RUNNING"
	case TaskStateCompleted:
		return "COMPLETED"
	case TaskStateFailed:
		return "FAILED"
	case TaskStateCancelled:
		return "CANCELLED"
	case TaskStateRetrying:
		return "RETRYING"
	default:
		return "UNKNOWN"
	}
}

func (s TaskState) IsTerminal() bool {
	return s == TaskStateCompleted || s == TaskStateFailed || s == TaskStateCancelled
}

type JobState int

const (
	JobStatePending JobState = iota
	JobStateQueued
	JobStateRunning
	JobStateCompleted
	JobStateFailed
	JobStateCancelled
	JobStateRetrying
)

func (s JobState) String() string {
	switch s {
	case JobStatePending:
		return "PENDING"
	case JobStateQueued:
		return "QUEUED"
	case JobStateRunning:
		return "RUNNING"
	case JobStateCompleted:
		return "COMPLETED"
	case JobStateFailed:
		return "FAILED"
	case JobStateCancelled:
		return "CANCELLED"
	case JobStateRetrying:
		return "RETRYING"
	default:
		return "UNKNOWN"
	}
}

func (s JobState) IsTerminal() bool {
	return s == JobStateCompleted || s == JobStateFailed || s == JobStateCancelled
}

type RetryPolicy struct {
	MaxRetries        int           `json:"max_retries"`
	InitialBackoff    time.Duration `json:"initial_backoff"`
	MaxBackoff        time.Duration `json:"max_backoff"`
	BackoffMultiplier float64       `json:"backoff_multiplier"`
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:        3,
		InitialBackoff:    time.Second,
		MaxBackoff:        30 * time.Second,
		BackoffMultiplier: 2.0,
	}
}

type Task struct {
	ID             string               `json:"id"`
	JobID          string               `json:"job_id"`
	Name           string               `json:"name"`
	Command        string               `json:"command"`
	Args           []string             `json:"args"`
	State          TaskState            `json:"state"`
	AssignedWorker string               `json:"assigned_worker,omitempty"`
	Attempt        int                  `json:"attempt"`
	MaxRetries     int                  `json:"max_retries"`
	ResourceReqs   ResourceRequirements `json:"resource_requirements"`
	Dependencies   []string             `json:"dependencies,omitempty"`
	CreatedAt      time.Time            `json:"created_at"`
	UpdatedAt      time.Time            `json:"updated_at"`
	Stdout         string               `json:"stdout,omitempty"`
	Stderr         string               `json:"stderr,omitempty"`
	ExitCode       int                  `json:"exit_code"`
	ErrorMessage   string               `json:"error_message,omitempty"`
}

type Job struct {
	ID           string      `json:"id"`
	Name         string      `json:"name"`
	Priority     int         `json:"priority"` // 0 = highest, 100 = lowest
	Tasks        []*Task     `json:"tasks"`
	State        JobState    `json:"state"`
	RetryPolicy  RetryPolicy `json:"retry_policy"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
	ErrorMessage string      `json:"error_message,omitempty"`
}

type DAGEdge struct {
	FromTask string `json:"from_task"`
	ToTask   string `json:"to_task"`
}

type DAG struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Priority    int         `json:"priority"`
	Tasks       []*Task     `json:"tasks"`
	Edges       []DAGEdge   `json:"edges"`
	RetryPolicy RetryPolicy `json:"retry_policy"`
}

func (j *Job) Validate() error {
	if j.Name == "" {
		return fmt.Errorf("job name is required")
	}
	if j.Priority < 0 || j.Priority > 100 {
		return fmt.Errorf("priority must be between 0 (highest) and 100 (lowest)")
	}
	if len(j.Tasks) == 0 {
		return fmt.Errorf("job must have at least one task")
	}
	for _, t := range j.Tasks {
		if t.Command == "" {
			return fmt.Errorf("task %q has no command", t.Name)
		}
	}
	return nil
}

func (j *Job) TaskByID(taskID string) *Task {
	for _, t := range j.Tasks {
		if t.ID == taskID {
			return t
		}
	}
	return nil
}

func (j *Job) AllTasksTerminal() bool {
	for _, t := range j.Tasks {
		if !t.State.IsTerminal() {
			return false
		}
	}
	return true
}

func (j *Job) HasFailedTasks() bool {
	for _, t := range j.Tasks {
		if t.State == TaskStateFailed {
			return true
		}
	}
	return false
}
