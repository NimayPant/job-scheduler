package grpcapi

import (
	"time"

	"github.com/NimayPant/job-scheduler/pkg/models"
	schedulerpb "github.com/NimayPant/job-scheduler/pkg/pb"
	"github.com/NimayPant/job-scheduler/pkg/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)


func toProtoJob(j *models.Job) *schedulerpb.Job {
	if j == nil {
		return nil
	}
	tasks := make([]*schedulerpb.Task, len(j.Tasks))
	for i, t := range j.Tasks {
		tasks[i] = toProtoTask(t)
	}
	return &schedulerpb.Job{
		Id:           j.ID,
		Name:         j.Name,
		Priority:     int32(j.Priority),
		Tasks:        tasks,
		State:        schedulerpb.JobState(j.State + 1), // PB enums start at 1 for PENDING
		RetryPolicy:  toProtoRetryPolicy(&j.RetryPolicy),
		CreatedAt:    timestamppb.New(j.CreatedAt),
		UpdatedAt:    timestamppb.New(j.UpdatedAt),
		ErrorMessage: j.ErrorMessage,
	}
}

func toProtoTask(t *models.Task) *schedulerpb.Task {
	if t == nil {
		return nil
	}
	return &schedulerpb.Task{
		Id:                   t.ID,
		JobId:                t.JobID,
		Name:                 t.Name,
		Command:              t.Command,
		Args:                 t.Args,
		State:                schedulerpb.TaskState(t.State + 1),
		AssignedWorker:       t.AssignedWorker,
		Attempt:              int32(t.Attempt),
		MaxRetries:           int32(t.MaxRetries),
		ResourceRequirements: toProtoResourceReqs(&t.ResourceReqs),
		Dependencies:         t.Dependencies,
		CreatedAt:            timestamppb.New(t.CreatedAt),
		UpdatedAt:            timestamppb.New(t.UpdatedAt),
		Stdout:               t.Stdout,
		Stderr:               t.Stderr,
		ExitCode:             int32(t.ExitCode),
		ErrorMessage:         t.ErrorMessage,
	}
}

func toProtoResourceReqs(r *models.ResourceRequirements) *schedulerpb.ResourceRequirements {
	if r == nil {
		return nil
	}
	return &schedulerpb.ResourceRequirements{
		CpuCores: int32(r.CPUCores),
		MemoryMb: r.MemoryMB,
		Gpus:     int32(r.GPUs),
		DiskMb:   r.DiskMB,
	}
}

func fromProtoResourceReqs(r *schedulerpb.ResourceRequirements) models.ResourceRequirements {
	if r == nil {
		return models.ResourceRequirements{}
	}
	return models.ResourceRequirements{
		CPUCores: int(r.CpuCores),
		MemoryMB: r.MemoryMb,
		GPUs:     int(r.Gpus),
		DiskMB:   r.DiskMb,
	}
}

func toProtoRetryPolicy(p *models.RetryPolicy) *schedulerpb.RetryPolicy {
	if p == nil {
		return nil
	}
	return &schedulerpb.RetryPolicy{
		MaxRetries:        int32(p.MaxRetries),
		InitialBackoffMs:  p.InitialBackoff.Milliseconds(),
		MaxBackoffMs:      p.MaxBackoff.Milliseconds(),
		BackoffMultiplier: p.BackoffMultiplier,
	}
}

func fromProtoRetryPolicy(p *schedulerpb.RetryPolicy) *models.RetryPolicy {
	if p == nil {
		return nil
	}
	return &models.RetryPolicy{
		MaxRetries:        int(p.MaxRetries),
		InitialBackoff:    time.Duration(p.InitialBackoffMs) * time.Millisecond,
		MaxBackoff:        time.Duration(p.MaxBackoffMs) * time.Millisecond,
		BackoffMultiplier: p.BackoffMultiplier,
	}
}

func toProtoWorkerInfo(w *store.WorkerRecord) *schedulerpb.WorkerInfo {
	if w == nil {
		return nil
	}
	return &schedulerpb.WorkerInfo{
		Id:      w.ID,
		Address: w.Address,
		State:   schedulerpb.WorkerState(w.State + 1),
		Resources: &schedulerpb.ResourceCapacity{
			Total:     toProtoResourceReqs(&w.Resources.Total),
			Available: toProtoResourceReqs(&w.Resources.Available),
		},
		RunningTaskIds: w.RunningTaskIDs,
		LastHeartbeat:  timestamppb.New(w.LastHeartbeat),
	}
}
