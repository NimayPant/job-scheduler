package grpcapi

import (
	"context"
	"log"

	"github.com/NimayPant/job-scheduler/pkg/models"
	schedulerpb "github.com/NimayPant/job-scheduler/pkg/pb"
	"github.com/NimayPant/job-scheduler/pkg/worker"
)

type WorkerGRPCHandler struct {
	schedulerpb.UnimplementedWorkerServiceServer
	w *worker.Worker
}

func NewWorkerGRPCHandler(w *worker.Worker) *WorkerGRPCHandler {
	return &WorkerGRPCHandler{w: w}
}

func (h *WorkerGRPCHandler) AssignTask(ctx context.Context, req *schedulerpb.AssignTaskRequest) (*schedulerpb.AssignTaskResponse, error) {

	task := &models.Task{
		ID:           req.Task.Id,
		JobID:        req.Task.JobId,
		Name:         req.Task.Name,
		Command:      req.Task.Command,
		Args:         req.Task.Args,
		State:        models.TaskState(req.Task.State - 1),
		MaxRetries:   int(req.Task.MaxRetries),
		ResourceReqs: fromProtoResourceReqs(req.Task.ResourceRequirements),
		Dependencies: req.Task.Dependencies,
	}

	if err := h.w.AssignTask(task); err != nil {
		log.Printf("task rejected: %s (%v)", req.Task.Id, err)
		return &schedulerpb.AssignTaskResponse{Accepted: false, Reason: err.Error()}, nil
	}
	return &schedulerpb.AssignTaskResponse{Accepted: true}, nil
}

func (h *WorkerGRPCHandler) CancelTask(ctx context.Context, req *schedulerpb.CancelTaskRequest) (*schedulerpb.CancelTaskResponse, error) {
	err := h.w.CancelTask(req.TaskId)
	return &schedulerpb.CancelTaskResponse{Success: err == nil}, err
}

type CoordinationClientAdapter struct {
	client schedulerpb.CoordinationServiceClient
}

func NewCoordinationClientAdapter(schedulerAddr string) (*CoordinationClientAdapter, error) {
	conn, err := DialAddress(schedulerAddr)
	if err != nil {
		return nil, err
	}
	return &CoordinationClientAdapter{
		client: schedulerpb.NewCoordinationServiceClient(conn),
	}, nil
}

func (a *CoordinationClientAdapter) Register(ctx context.Context, workerID, address string, resources models.ResourceCapacity) error {
	resp, err := a.client.RegisterWorker(ctx, &schedulerpb.RegisterWorkerRequest{
		WorkerId: workerID,
		Address:  address,
		Resources: &schedulerpb.ResourceCapacity{
			Total:     toProtoResourceReqs(&resources.Total),
			Available: toProtoResourceReqs(&resources.Available),
		},
	})
	if err != nil {
		return err
	}
	if !resp.Accepted {
		return &NotLeaderError{LeaderAddr: resp.LeaderAddress}
	}
	return nil
}

func (a *CoordinationClientAdapter) SendHeartbeat(ctx context.Context, workerID string, resources models.ResourceCapacity, runningTaskIDs []string) error {
	_, err := a.client.Heartbeat(ctx, &schedulerpb.HeartbeatRequest{
		WorkerId: workerID,
		Resources: &schedulerpb.ResourceCapacity{
			Total:     toProtoResourceReqs(&resources.Total),
			Available: toProtoResourceReqs(&resources.Available),
		},
		RunningTaskIds: runningTaskIDs,
	})
	return err
}

func (a *CoordinationClientAdapter) ReportTaskStatus(ctx context.Context, workerID, taskID, jobID string, state models.TaskState, exitCode int, stdout, stderr, errMsg string) error {
	_, err := a.client.ReportTaskStatus(ctx, &schedulerpb.ReportTaskStatusRequest{
		WorkerId:     workerID,
		TaskId:       taskID,
		JobId:        jobID,
		State:        schedulerpb.TaskState(state + 1),
		ExitCode:     int32(exitCode),
		Stdout:       stdout,
		Stderr:       stderr,
		ErrorMessage: errMsg,
	})
	return err
}

type NotLeaderError struct {
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	return "not leader, redirect to " + e.LeaderAddr
}
