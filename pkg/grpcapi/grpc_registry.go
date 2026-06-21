package grpcapi

import (
	"fmt"
	"log"
	"net"

	"github.com/NimayPant/job-scheduler/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func DialAddress(address string) (*grpc.ClientConn, error) {
	return grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func StartGRPCServer(address string, s *grpc.Server) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}
	log.Printf("gRPC server listening on %s", address)
	return s.Serve(lis)
}

func NewGRPCServer() *grpc.Server {
	return grpc.NewServer()
}

func RegisterSchedulerServices(s *grpc.Server, srv *SchedulerServer) {
	schedulerpb.RegisterSchedulerServiceServer(s, srv)
	schedulerpb.RegisterCoordinationServiceServer(s, srv)
}

func RegisterWorkerGRPCService(s *grpc.Server, srv *WorkerGRPCHandler) {
	schedulerpb.RegisterWorkerServiceServer(s, srv)
}
