package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/NimayPant/job-scheduler/pkg/store"
)

type NodeConfig struct {
	NodeID        string
	BindAddress   string
	DataDir       string
	Bootstrap     bool
	JoinAddress   string   // Leader address to join (empty if bootstrapping)
	PeerAddresses []string // Initial peer addresses for bootstrap
}

type RaftNode struct {
	raft   *hraft.Raft
	fsm    *FSM
	config NodeConfig
}

func NewRaftNode(cfg NodeConfig, boltStore *store.BoltStore) (*RaftNode, error) {
	fsm := NewFSM(boltStore)

	raftConfig := hraft.DefaultConfig()
	raftConfig.LocalID = hraft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = 1000 * time.Millisecond
	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond
	raftConfig.CommitTimeout = 200 * time.Millisecond
	raftConfig.SnapshotInterval = 60 * time.Second
	raftConfig.SnapshotThreshold = 1024

	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create raft dir: %w", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	snapshotStore, err := hraft.NewFileSnapshotStore(raftDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address %s: %w", cfg.BindAddress, err)
	}
	transport, err := hraft.NewTCPTransport(cfg.BindAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	r, err := hraft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft instance: %w", err)
	}

	node := &RaftNode{
		raft:   r,
		fsm:    fsm,
		config: cfg,
	}

	if cfg.Bootstrap {
		servers := []hraft.Server{
			{
				ID:      hraft.ServerID(cfg.NodeID),
				Address: hraft.ServerAddress(cfg.BindAddress),
			},
		}
		for i, addr := range cfg.PeerAddresses {
			servers = append(servers, hraft.Server{
				ID:      hraft.ServerID(fmt.Sprintf("node-%d", i+1)),
				Address: hraft.ServerAddress(addr),
			})
		}
		future := r.BootstrapCluster(hraft.Configuration{Servers: servers})
		if err := future.Error(); err != nil {
			log.Printf("bootstrap error (may be already bootstrapped): %v", err)
		}
	}

	return node, nil
}

func (n *RaftNode) IsLeader() bool {
	return n.raft.State() == hraft.Leader
}

func (n *RaftNode) LeaderAddress() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

func (n *RaftNode) LeaderID() string {
	id, _ := n.raft.LeaderWithID()
	return string(id)
}

func (n *RaftNode) FSM() *FSM {
	return n.fsm
}

func (n *RaftNode) Apply(cmd Command, timeout time.Duration) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}
	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}
	resp := future.Response()
	if err, ok := resp.(error); ok {
		return err
	}
	return nil
}

func (n *RaftNode) ApplyCommand(cmdType CommandType, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return n.Apply(Command{
		Type:    cmdType,
		Payload: json.RawMessage(data),
	}, 5*time.Second)
}

func (n *RaftNode) AddVoter(nodeID, address string) error {
	future := n.raft.AddVoter(
		hraft.ServerID(nodeID),
		hraft.ServerAddress(address),
		0,
		10*time.Second,
	)
	return future.Error()
}

func (n *RaftNode) RemoveServer(nodeID string) error {
	future := n.raft.RemoveServer(hraft.ServerID(nodeID), 0, 10*time.Second)
	return future.Error()
}

func (n *RaftNode) Shutdown() error {
	future := n.raft.Shutdown()
	return future.Error()
}

func (n *RaftNode) Stats() map[string]string {
	return n.raft.Stats()
}

func (n *RaftNode) State() string {
	return n.raft.State().String()
}
