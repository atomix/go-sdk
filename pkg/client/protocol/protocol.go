package protocol

import "github.com/atomix/atomix-go-client/proto/atomix/partition"

type Protocol interface {
	Spec() *partition.PartitionGroupSpec
}
