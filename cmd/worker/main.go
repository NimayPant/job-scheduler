package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/NimayPant/job-scheduler/pkg/grpcapi"
	"github.com/NimayPant/job-scheduler/pkg/worker"
)

func main() {
	workerID := flag.String("id", "", "Worker ID (auto-generated if empty)")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:9000", "gRPC listen address for task dispatch")
	schedulerAddr := flag.String("scheduler", "127.0.0.1:8000", "Scheduler gRPC address")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if *workerID == "" {
		*workerID = "worker-" + uuid.New().String()[:8]
	}

	// Auto-detect resources.
	resources := worker.DetectResources()
	log.Printf("detected resources: %s", resources.Total)

	// Create coordination client (gRPC adapter).
	coordClient, err := grpcapi.NewCoordinationClientAdapter(*schedulerAddr)
	if err != nil {
		log.Fatalf("failed to connect to scheduler: %v", err)
	}

	// Create worker.
	w := worker.NewWorker(*workerID, *grpcAddr, resources, coordClient)

	// Start worker (registers + heartbeat loop).
	if err := w.Start(context.Background()); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}
	defer w.Stop()

	// Start gRPC server for task dispatch (Scheduler → Worker).
	grpcServer := grpcapi.NewGRPCServer()
	workerHandler := grpcapi.NewWorkerGRPCHandler(w)
	grpcapi.RegisterWorkerGRPCService(grpcServer, workerHandler)

	go func() {
		if err := grpcapi.StartGRPCServer(*grpcAddr, grpcServer); err != nil {
			log.Fatalf("worker gRPC server failed: %v", err)
		}
	}()

	log.Printf("worker %s started (grpc=%s scheduler=%s)", *workerID, *grpcAddr, *schedulerAddr)

	// Wait for shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down worker...")
	grpcServer.GracefulStop()
	log.Println("worker shutdown complete")
}
