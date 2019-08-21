package set

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"google.golang.org/grpc"
)

type SetClient interface {
	GetSet(ctx context.Context, name string, opts ...session.SessionOption) (Set, error)
}

type Set interface {
	primitive.Primitive
	Add(ctx context.Context, value string) (bool, error)
	Remove(ctx context.Context, value string) (bool, error)
	Contains(ctx context.Context, value string) (bool, error)
	Len(ctx context.Context) (int, error)
	Clear(ctx context.Context) error
	Watch(ctx context.Context, ch chan<- *SetEvent, opts ...WatchOption) error
}

type SetEventType string

const (
	EVENT_ADDED   SetEventType = "added"
	EVENT_REMOVED SetEventType = "removed"
)

type SetEvent struct {
	Type  SetEventType
	Value string
}

func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.SessionOption) (Set, error) {
	results, err := util.ExecuteOrderedAsync(len(partitions), func(i int) (interface{}, error) {
		return newPartition(ctx, partitions[i], name, opts...)
	})
	if err != nil {
		return nil, err
	}

	sets := make([]Set, len(results))
	for i, result := range results {
		sets[i] = result.(Set)
	}

	return &set{
		name:       name,
		partitions: sets,
	}, nil
}

type set struct {
	name       primitive.Name
	partitions []Set
}

func (s *set) Name() primitive.Name {
	return s.name
}

func (s *set) getPartition(key string) (Set, error) {
	i, err := util.GetPartitionIndex(key, len(s.partitions))
	if err != nil {
		return nil, err
	}
	return s.partitions[i], nil
}

func (s *set) Add(ctx context.Context, value string) (bool, error) {
	partition, err := s.getPartition(value)
	if err != nil {
		return false, err
	}
	return partition.Add(ctx, value)
}

func (s *set) Remove(ctx context.Context, value string) (bool, error) {
	partition, err := s.getPartition(value)
	if err != nil {
		return false, err
	}
	return partition.Remove(ctx, value)
}

func (s *set) Contains(ctx context.Context, value string) (bool, error) {
	partition, err := s.getPartition(value)
	if err != nil {
		return false, err
	}
	return partition.Contains(ctx, value)
}

func (s *set) Len(ctx context.Context) (int, error) {
	results, err := util.ExecuteAsync(len(s.partitions), func(i int) (interface{}, error) {
		return s.partitions[i].Len(ctx)
	})
	if err != nil {
		return 0, err
	}

	total := 0
	for _, result := range results {
		total += result.(int)
	}
	return total, nil
}

func (s *set) Clear(ctx context.Context) error {
	return util.IterAsync(len(s.partitions), func(i int) error {
		return s.partitions[i].Clear(ctx)
	})
}

func (s *set) Watch(ctx context.Context, ch chan<- *SetEvent, opts ...WatchOption) error {
	return util.IterAsync(len(s.partitions), func(i int) error {
		return s.partitions[i].Watch(ctx, ch, opts...)
	})
}

func (s *set) Close() error {
	return util.IterAsync(len(s.partitions), func(i int) error {
		return s.partitions[i].Close()
	})
}

func (s *set) Delete() error {
	return util.IterAsync(len(s.partitions), func(i int) error {
		return s.partitions[i].Delete()
	})
}
