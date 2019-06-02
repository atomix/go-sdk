package raft

import (
	"github.com/atomix/atomix-go-client/proto/atomix/partition"
	"github.com/golang/protobuf/ptypes"
	"time"
)

type Protocol struct {
	ElectionTimeout   *time.Duration
	HeartbeatInterval *time.Duration
}

func (p *Protocol) Spec() *partition.PartitionGroupSpec {
	raft := &partition.RaftPartitionGroup{}
	if p.ElectionTimeout != nil {
		raft.ElectionTimeout = ptypes.DurationProto(*p.ElectionTimeout)
	}
	if p.HeartbeatInterval != nil {
		raft.HeartbeatInterval = ptypes.DurationProto(*p.HeartbeatInterval)
	}

	return &partition.PartitionGroupSpec{
		Group: &partition.PartitionGroupSpec_Raft{
			Raft: raft,
		},
	}
}
