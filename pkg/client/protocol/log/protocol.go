package log

import (
	"github.com/atomix/atomix-go-client/proto/atomix/partition"
)

type MemberGroupStrategy string

const (
	HostAwareMemberGroupStrategy MemberGroupStrategy = "host_aware"
	RackAwareMemberGroupStrategy MemberGroupStrategy = "rack_aware"
	ZoneAwareMemberGroupStrategy MemberGroupStrategy = "zone_aware"
)

type Protocol struct {
	MemberGroupStrategy *MemberGroupStrategy
}

func (p *Protocol) Spec() *partition.PartitionGroupSpec {
	log := &partition.DistributedLogPartitionGroup{}
	if p.MemberGroupStrategy != nil {
		switch *p.MemberGroupStrategy {
		case HostAwareMemberGroupStrategy:
			log.MemberGroupStrategy = partition.MemberGroupStrategy_HOST_AWARE
		case RackAwareMemberGroupStrategy:
			log.MemberGroupStrategy = partition.MemberGroupStrategy_RACK_AWARE
		case ZoneAwareMemberGroupStrategy:
			log.MemberGroupStrategy = partition.MemberGroupStrategy_ZONE_AWARE
		default:
		}
	}
	return &partition.PartitionGroupSpec{
		Group: &partition.PartitionGroupSpec_Log{
			Log: log,
		},
	}
}
