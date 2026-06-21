package grpcapi

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/NimayPant/job-scheduler/pkg/models"
	schedulerpb "github.com/NimayPant/job-scheduler/pkg/pb"
	"github.com/NimayPant/job-scheduler/pkg/raft"
	"github.com/NimayPant/job-scheduler/pkg/scheduler"
	"github.com/NimayPant/job-scheduler/pkg/store"
)

type SchedulerServer struct {
	schedulerpb.UnimplementedSchedulerServiceServer
	schedulerpb.UnimplementedCoordinationServiceServer

	node      *raft.RaftNode
	scheduler *scheduler.Scheduler
}

func NewSchedulerServer(node *raft.RaftNode, sched *scheduler.Scheduler) *SchedulerServer {
	return &SchedulerServer{node: node, scheduler: sched}
}


func (s *SchedulerServer) SubmitJob(ctx context.Context, req *schedulerpb.SubmitJobRequest) (*schedulerpb.SubmitJobResponse, error) {
	if !s.node.IsLeader() {
		return nil, fmt.Errorf("not leader, redirect to %s", s.node.LeaderAddress())
	}

	jobID := uuid.New().String()
	now := time.Now()

	tasks := make([]*models.Task, len(req.Tasks))
	for i, t := range req.Tasks {
		tasks[i] = &models.Task{
			ID:           uuid.New().String(),
			JobID:        jobID,
			Name:         t.Name,
			Command:      t.Command,
			Args:         t.Args,
			State:        models.TaskStatePending,
			MaxRetries:   int(t.MaxRetries),
			ResourceReqs: fromProtoResourceReqs(t.ResourceRequirements),
			Dependencies: t.Dependencies,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
	}

	retryPolicy := models.DefaultRetryPolicy()
	if req.RetryPolicy != nil {
		retryPolicy = *fromProtoRetryPolicy(req.RetryPolicy)
	}

	job := &models.Job{
		ID:          jobID,
		Name:        req.Name,
		Priority:    int(req.Priority),
		Tasks:       tasks,
		State:       models.JobStatePending,
		RetryPolicy: retryPolicy,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := job.Validate(); err != nil {
		return nil, fmt.Errorf("validation: %w", err)
	}

	workers := s.node.FSM().ListWorkers()
	totalCap, availCap := scheduler.EstimateClusterCapacity(workers)
	log.Printf("Cluster capacity before SubmitJob - Total: %s, Available: %s", totalCap, availCap)

	for _, t := range job.Tasks {
		if !scheduler.CanFitAnywhere(t, workers) {
			return nil, fmt.Errorf("task %s requires resources that exceed any single worker in the cluster", t.Name)
		}
	}

	if err := s.node.ApplyCommand(raft.CmdSubmitJob, raft.SubmitJobPayload{Job: job}); err != nil {
		return nil, fmt.Errorf("raft apply: %w", err)
	}

	s.scheduler.EnqueueJob(job)
	log.Printf("job submitted: %s (%s)", jobID, req.Name)
	return &schedulerpb.SubmitJobResponse{JobId: jobID}, nil
}

func (s *SchedulerServer) SubmitDAG(ctx context.Context, req *schedulerpb.SubmitDAGRequest) (*schedulerpb.SubmitDAGResponse, error) {
	if !s.node.IsLeader() {
		return nil, fmt.Errorf("not leader, redirect to %s", s.node.LeaderAddress())
	}

	jobID := uuid.New().String()
	dagID := uuid.New().String()
	now := time.Now()

	def := req.Dag

	tasks := make([]*models.Task, len(def.Tasks))
	taskMap := make(map[string]*models.Task)
	for i, t := range def.Tasks {
		tasks[i] = &models.Task{
			ID:           uuid.New().String(),
			JobID:        jobID,
			Name:         t.Name,
			Command:      t.Command,
			Args:         t.Args,
			State:        models.TaskStatePending,
			MaxRetries:   int(t.MaxRetries),
			ResourceReqs: fromProtoResourceReqs(t.ResourceRequirements),
			Dependencies: t.Dependencies,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		taskMap[t.Name] = tasks[i]
	}

	edges := make([]models.DAGEdge, len(def.Edges))
	for i, edge := range def.Edges {
		fromTask, ok := taskMap[edge.FromTask]
		toTask, ok2 := taskMap[edge.ToTask]
		if !ok || !ok2 {
			return nil, fmt.Errorf("invalid edge: %s -> %s", edge.FromTask, edge.ToTask)
		}
		toTask.Dependencies = append(toTask.Dependencies, fromTask.ID)
		edges[i] = models.DAGEdge{
			FromTask: fromTask.ID,
			ToTask:   toTask.ID,
		}
	}

	retryPolicy := models.DefaultRetryPolicy()
	if def.RetryPolicy != nil {
		retryPolicy = *fromProtoRetryPolicy(def.RetryPolicy)
	}

	job := &models.Job{
		ID:          jobID,
		Name:        def.Name,
		Priority:    int(def.Priority),
		Tasks:       tasks,
		State:       models.JobStatePending,
		RetryPolicy: retryPolicy,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	dag := &models.DAG{
		ID:          dagID,
		Name:        def.Name,
		Priority:    int(def.Priority),
		Tasks:       tasks,
		Edges:       edges,
		RetryPolicy: retryPolicy,
	}

	if err := scheduler.ValidateDAG(dag); err != nil {
		return nil, fmt.Errorf("invalid DAG: %w", err)
	}
	if _, err := scheduler.TopologicalSort(dag); err != nil {
		return nil, fmt.Errorf("DAG topological sort failed: %w", err)
	}

	workers := s.node.FSM().ListWorkers()
	totalCap, availCap := scheduler.EstimateClusterCapacity(workers)
	log.Printf("Cluster capacity before SubmitDAG - Total: %s, Available: %s", totalCap, availCap)

	for _, t := range job.Tasks {
		if !scheduler.CanFitAnywhere(t, workers) {
			return nil, fmt.Errorf("task %s requires resources that exceed any single worker in the cluster", t.Name)
		}
	}

	if err := s.node.ApplyCommand(raft.CmdSubmitDAG, raft.SubmitDAGPayload{DAG: dag, Job: job}); err != nil {
		return nil, fmt.Errorf("raft apply: %w", err)
	}

	s.scheduler.RegisterDAG(dag, job)
	log.Printf("DAG submitted: %s (Job: %s)", dagID, jobID)
	return &schedulerpb.SubmitDAGResponse{DagId: dagID, JobId: jobID}, nil
}

func (s *SchedulerServer) GetJobStatus(ctx context.Context, req *schedulerpb.GetJobStatusRequest) (*schedulerpb.GetJobStatusResponse, error) {
	job, ok := s.node.FSM().GetJob(req.JobId)
	if !ok {
		return nil, fmt.Errorf("job %s not found", req.JobId)
	}
	return &schedulerpb.GetJobStatusResponse{Job: toProtoJob(job)}, nil
}

func (s *SchedulerServer) CancelJob(ctx context.Context, req *schedulerpb.CancelJobRequest) (*schedulerpb.CancelJobResponse, error) {
	if !s.node.IsLeader() {
		return nil, fmt.Errorf("not leader")
	}
	if err := s.node.ApplyCommand(raft.CmdCancelJob, raft.CancelJobPayload{JobID: req.JobId, Timestamp: time.Now()}); err != nil {
		return nil, err
	}
	log.Printf("job cancelled: %s", req.JobId)
	return &schedulerpb.CancelJobResponse{Success: true}, nil
}

func (s *SchedulerServer) ListJobs(ctx context.Context, req *schedulerpb.ListJobsRequest) (*schedulerpb.ListJobsResponse, error) {
	jobs := s.node.FSM().ListJobs()
	if req.FilterState != schedulerpb.JobState_JOB_STATE_UNSPECIFIED {
		var filtered []*models.Job
		targetState := models.JobState(req.FilterState - 1)
		for _, j := range jobs {
			if j.State == targetState {
				filtered = append(filtered, j)
			}
		}
		jobs = filtered
	}
	if req.Limit > 0 && int32(len(jobs)) > req.Limit {
		jobs = jobs[:req.Limit]
	}
	protoJobs := make([]*schedulerpb.Job, len(jobs))
	for i, j := range jobs {
		protoJobs[i] = toProtoJob(j)
	}
	return &schedulerpb.ListJobsResponse{Jobs: protoJobs}, nil
}

func (s *SchedulerServer) ListWorkers(ctx context.Context, req *schedulerpb.ListWorkersRequest) (*schedulerpb.ListWorkersResponse, error) {
	records := s.node.FSM().ListWorkers()
	workers := make([]*schedulerpb.WorkerInfo, len(records))
	for i, w := range records {
		workers[i] = toProtoWorkerInfo(w)
	}
	return &schedulerpb.ListWorkersResponse{Workers: workers}, nil
}


func (s *SchedulerServer) RegisterWorker(ctx context.Context, req *schedulerpb.RegisterWorkerRequest) (*schedulerpb.RegisterWorkerResponse, error) {
	if !s.node.IsLeader() {
		return &schedulerpb.RegisterWorkerResponse{Accepted: false, LeaderAddress: s.node.LeaderAddress()}, nil
	}

	record := &store.WorkerRecord{
		ID:            req.WorkerId,
		Address:       req.Address,
		State:         models.WorkerStateActive,
		Resources:     models.ResourceCapacity{Total: fromProtoResourceReqs(req.Resources.Total), Available: fromProtoResourceReqs(req.Resources.Available)},
		LastHeartbeat: time.Now(),
	}

	if err := s.node.ApplyCommand(raft.CmdRegisterWorker, raft.RegisterWorkerPayload{Worker: record}); err != nil {
		return nil, fmt.Errorf("register worker: %w", err)
	}
	log.Printf("worker registered: %s at %s", req.WorkerId, req.Address)
	return &schedulerpb.RegisterWorkerResponse{Accepted: true}, nil
}

func (s *SchedulerServer) Heartbeat(ctx context.Context, req *schedulerpb.HeartbeatRequest) (*schedulerpb.HeartbeatResponse, error) {
	if !s.node.IsLeader() {
		return &schedulerpb.HeartbeatResponse{Acknowledged: false}, nil
	}

	err := s.node.ApplyCommand(raft.CmdUpdateWorker, raft.UpdateWorkerPayload{
		WorkerID:      req.WorkerId,
		Resources:     models.ResourceCapacity{Total: fromProtoResourceReqs(req.Resources.Total), Available: fromProtoResourceReqs(req.Resources.Available)},
		RunningTasks:  req.RunningTaskIds,
		LastHeartbeat: time.Now(),
	})
	if err != nil {
		return nil, err
	}
	return &schedulerpb.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *SchedulerServer) ReportTaskStatus(ctx context.Context, req *schedulerpb.ReportTaskStatusRequest) (*schedulerpb.ReportTaskStatusResponse, error) {
	if !s.node.IsLeader() {
		return nil, fmt.Errorf("not leader")
	}

	state := models.TaskState(req.State - 1)
	err := s.node.ApplyCommand(raft.CmdUpdateTaskState, raft.UpdateTaskStatePayload{
		TaskID:       req.TaskId,
		JobID:        req.JobId,
		State:        state,
		ExitCode:     int(req.ExitCode),
		Stdout:       req.Stdout,
		Stderr:       req.Stderr,
		ErrorMessage: req.ErrorMessage,
		Timestamp:    time.Now(),
	})
	if err != nil {
		return nil, err
	}

	s.scheduler.HandleTaskCompletion(req.TaskId, req.JobId, state)
	log.Printf("task status updated: %s -> %s", req.TaskId, state)
	return &schedulerpb.ReportTaskStatusResponse{Acknowledged: true}, nil
}


type GRPCTaskDispatcher struct{}

func NewGRPCTaskDispatcher() *GRPCTaskDispatcher { return &GRPCTaskDispatcher{} }

func (d *GRPCTaskDispatcher) DispatchTask(workerAddress string, task *models.Task) error {
	conn, err := DialAddress(workerAddress)
	if err != nil {
		return fmt.Errorf("connect to worker %s: %w", workerAddress, err)
	}
	defer conn.Close()

	client := schedulerpb.NewWorkerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.AssignTask(ctx, &schedulerpb.AssignTaskRequest{Task: toProtoTask(task)})
	if err != nil {
		return err
	}
	if !resp.Accepted {
		return fmt.Errorf("worker rejected: %s", resp.Reason)
	}
	return nil
}

func (d *GRPCTaskDispatcher) CancelTaskOnWorker(workerAddress string, taskID string) error {
	conn, err := DialAddress(workerAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := schedulerpb.NewWorkerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.CancelTask(ctx, &schedulerpb.CancelTaskRequest{TaskId: taskID})
	return err
}
