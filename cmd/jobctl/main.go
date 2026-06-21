package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/NimayPant/job-scheduler/pkg/grpcapi"
	"github.com/NimayPant/job-scheduler/pkg/models"
	schedulerpb "github.com/NimayPant/job-scheduler/pkg/pb"
)

func main() {
	schedulerAddr := flag.String("scheduler", "127.0.0.1:8000", "Scheduler gRPC address")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	conn, err := grpcapi.DialAddress(*schedulerAddr)
	if err != nil {
		log.Fatalf("failed to connect to scheduler: %v", err)
	}
	defer conn.Close()

	client := schedulerpb.NewSchedulerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch args[0] {
	case "submit":
		cmdSubmit(ctx, client, args[1:])
	case "status":
		cmdStatus(ctx, client, args[1:])
	case "cancel":
		cmdCancel(ctx, client, args[1:])
	case "list":
		cmdList(ctx, client, args[1:])
	case "workers":
		cmdWorkers(ctx, client)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: jobctl [--scheduler ADDR] <command> [args]

Commands:
  submit   --name NAME --priority N --cmd COMMAND [--args A1,A2] [--retries N]
  status   --job-id ID
  cancel   --job-id ID
  list     [--state STATE] [--limit N]
  workers`)
}

func cmdSubmit(ctx context.Context, client schedulerpb.SchedulerServiceClient, args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	name := fs.String("name", "", "Job name")
	priority := fs.Int("priority", 50, "Priority (0=highest, 100=lowest)")
	cmd := fs.String("cmd", "", "Command to execute")
	taskArgs := fs.String("args", "", "Comma-separated arguments")
	retries := fs.Int("retries", 3, "Max retries per task")
	cpus := fs.Int("cpus", 1, "CPU cores required")
	memMB := fs.Int64("mem", 256, "Memory MB required")
	_ = fs.Parse(args)

	if *name == "" || *cmd == "" {
		fmt.Fprintln(os.Stderr, "error: --name and --cmd are required")
		os.Exit(1)
	}

	var cmdArgs []string
	if *taskArgs != "" {
		cmdArgs = strings.Split(*taskArgs, ",")
	}

	retryPolicy := models.DefaultRetryPolicy()
	retryPolicy.MaxRetries = *retries

	resp, err := client.SubmitJob(ctx, &schedulerpb.SubmitJobRequest{
		Name:     *name,
		Priority: int32(*priority),
		Tasks: []*schedulerpb.Task{
			{
				Name:       *name + "-task-0",
				Command:    *cmd,
				Args:       cmdArgs,
				MaxRetries: int32(*retries),
				ResourceRequirements: &schedulerpb.ResourceRequirements{
					CpuCores: int32(*cpus),
					MemoryMb: *memMB,
				},
			},
		},
		RetryPolicy: &schedulerpb.RetryPolicy{
			MaxRetries:        int32(retryPolicy.MaxRetries),
			InitialBackoffMs:  retryPolicy.InitialBackoff.Milliseconds(),
			MaxBackoffMs:      retryPolicy.MaxBackoff.Milliseconds(),
			BackoffMultiplier: retryPolicy.BackoffMultiplier,
		},
	})
	if err != nil {
		log.Fatalf("submit failed: %v", err)
	}
	fmt.Printf("Job submitted: %s\n", resp.JobId)
}

func cmdStatus(ctx context.Context, client schedulerpb.SchedulerServiceClient, args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	jobID := fs.String("job-id", "", "Job ID")
	_ = fs.Parse(args)

	if *jobID == "" {
		fmt.Fprintln(os.Stderr, "error: --job-id is required")
		os.Exit(1)
	}

	resp, err := client.GetJobStatus(ctx, &schedulerpb.GetJobStatusRequest{JobId: *jobID})
	if err != nil {
		log.Fatalf("status failed: %v", err)
	}

	data, _ := json.MarshalIndent(resp.Job, "", "  ")
	fmt.Println(string(data))
}

func cmdCancel(ctx context.Context, client schedulerpb.SchedulerServiceClient, args []string) {
	fs := flag.NewFlagSet("cancel", flag.ExitOnError)
	jobID := fs.String("job-id", "", "Job ID")
	_ = fs.Parse(args)

	if *jobID == "" {
		fmt.Fprintln(os.Stderr, "error: --job-id is required")
		os.Exit(1)
	}

	resp, err := client.CancelJob(ctx, &schedulerpb.CancelJobRequest{JobId: *jobID})
	if err != nil {
		log.Fatalf("cancel failed: %v", err)
	}
	fmt.Printf("Cancelled: %v\n", resp.Success)
}

func cmdList(ctx context.Context, client schedulerpb.SchedulerServiceClient, args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	limit := fs.Int("limit", 20, "Max results")
	_ = fs.Parse(args)

	resp, err := client.ListJobs(ctx, &schedulerpb.ListJobsRequest{Limit: int32(*limit)})
	if err != nil {
		log.Fatalf("list failed: %v", err)
	}

	if len(resp.Jobs) == 0 {
		fmt.Println("No jobs found.")
		return
	}

	fmt.Printf("%-38s %-20s %-10s %-8s %-5s\n", "ID", "NAME", "STATE", "PRIORITY", "TASKS")
	fmt.Println(strings.Repeat("-", 85))
	for _, j := range resp.Jobs {
		fmt.Printf("%-38s %-20s %-10s %-8d %-5d\n", j.Id, j.Name, j.State, j.Priority, len(j.Tasks))
	}
}

func cmdWorkers(ctx context.Context, client schedulerpb.SchedulerServiceClient) {
	resp, err := client.ListWorkers(ctx, &schedulerpb.ListWorkersRequest{})
	if err != nil {
		log.Fatalf("workers failed: %v", err)
	}

	if len(resp.Workers) == 0 {
		fmt.Println("No workers registered.")
		return
	}

	fmt.Printf("%-20s %-25s %-10s %-10s %-10s %-20s\n", "ID", "ADDRESS", "STATE", "CPU", "MEM(MB)", "LAST_HEARTBEAT")
	fmt.Println(strings.Repeat("-", 100))
	for _, w := range resp.Workers {
		fmt.Printf("%-20s %-25s %-10s %-10d %-10d %-20s\n",
			w.Id, w.Address, w.State,
			w.Resources.Available.CpuCores,
			w.Resources.Available.MemoryMb,
			w.LastHeartbeat.AsTime().Format("15:04:05"),
		)
	}
}
