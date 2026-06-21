package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/NimayPant/job-scheduler/pkg/grpcapi"
	"github.com/NimayPant/job-scheduler/pkg/raft"
	"github.com/NimayPant/job-scheduler/pkg/scheduler"
	"github.com/NimayPant/job-scheduler/pkg/store"
)

func main() {
	nodeID := flag.String("id", "node-0", "Raft node ID")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7000", "Raft bind address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:8000", "gRPC listen address")
	dataDir := flag.String("data-dir", "./data", "Data directory")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap a new Raft cluster")
	joinAddr := flag.String("join", "", "Leader gRPC address to join")
	peers := flag.String("peers", "", "Comma-separated peer Raft addresses for bootstrap")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	nodeDir := fmt.Sprintf("%s/%s", *dataDir, *nodeID)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	boltStore, err := store.NewBoltStore(fmt.Sprintf("%s/scheduler.db", nodeDir))
	if err != nil {
		log.Fatalf("failed to open bolt store: %v", err)
	}
	defer boltStore.Close()

	var peerAddrs []string
	if *peers != "" {
		peerAddrs = strings.Split(*peers, ",")
	}

	raftNode, err := raft.NewRaftNode(raft.NodeConfig{
		NodeID:        *nodeID,
		BindAddress:   *raftAddr,
		DataDir:       nodeDir,
		Bootstrap:     *bootstrap,
		JoinAddress:   *joinAddr,
		PeerAddresses: peerAddrs,
	}, boltStore)
	if err != nil {
		log.Fatalf("failed to create raft node: %v", err)
	}

	dispatcher := grpcapi.NewGRPCTaskDispatcher()
	sched := scheduler.NewScheduler(raftNode, dispatcher)
	sched.Start()
	defer sched.Stop()

	grpcServer := grpcapi.NewGRPCServer()
	schedulerServer := grpcapi.NewSchedulerServer(raftNode, sched)
	grpcapi.RegisterSchedulerServices(grpcServer, schedulerServer)

	go func() {
		if err := grpcapi.StartGRPCServer(*grpcAddr, grpcServer); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	log.Printf("scheduler node %s started (raft=%s grpc=%s bootstrap=%v)", *nodeID, *raftAddr, *grpcAddr, *bootstrap)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	grpcServer.GracefulStop()
	raftNode.Shutdown()
	log.Println("shutdown complete")
}
