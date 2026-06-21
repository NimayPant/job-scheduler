package raft

import (
	"encoding/json"

	hraft "github.com/hashicorp/raft"
)

type FSMSnapshot struct {
	state *fsmState
}

func (s *FSMSnapshot) Persist(sink hraft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.state)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *FSMSnapshot) Release() {}
