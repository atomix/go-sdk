package set

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"google.golang.org/grpc"
)

func NewSet(namespace string, name string, partitions []*grpc.ClientConn, opts ...session.Option) (*Set, error) {
	iter := make([]interface{}, len(partitions))
	for i, partition := range partitions {
		iter[i] = partition
	}

	results, err := util.ExecuteAllAsync(iter, func(arg interface{}) (interface{}, error) {
		return newPartition(arg.(*grpc.ClientConn), namespace, name, opts...)
	})

	if err != nil {
		return nil, err
	}

	setPartitions := make([]*setPartition, len(results))
	for i, result := range results {
		setPartitions[i] = result.(*setPartition)
	}

	return &Set{
		Namespace:      namespace,
		Name:           name,
		partitions:     setPartitions,
		partitionsIter: results,
	}, nil
}

type Set struct {
	Interface
	Namespace      string
	Name           string
	partitions     []*setPartition
	partitionsIter []interface{}
}

func (s *Set) getPartition(key string) (*setPartition, error) {
	i, err := util.GetPartitionIndex(key, len(s.partitions))
	if err != nil {
		return nil, err
	}
	return s.partitions[i], nil
}

func (s *Set) Add(ctx context.Context, value string) (bool, error) {
	partition, err := s.getPartition(value)
	if err != nil {
		return false, err
	}
	return partition.Add(ctx, value)
}

func (s *Set) Remove(ctx context.Context, value string) (bool, error) {
	partition, err := s.getPartition(value)
	if err != nil {
		return false, err
	}
	return partition.Remove(ctx, value)
}

func (s *Set) Contains(ctx context.Context, value string) (bool, error) {
	partition, err := s.getPartition(value)
	if err != nil {
		return false, err
	}
	return partition.Contains(ctx, value)
}

func (s *Set) Size(ctx context.Context) (int, error) {
	results, err := util.ExecuteAllAsync(s.partitionsIter, func(arg interface{}) (interface{}, error) {
		return arg.(*setPartition).Size(ctx)
	})
	if err != nil {
		return 0, err
	}

	size := 0
	for _, result := range results {
		size += result.(int)
	}
	return size, nil
}

func (s *Set) Clear(ctx context.Context) error {
	_, err := util.ExecuteAllAsync(s.partitionsIter, func(arg interface{}) (interface{}, error) {
		return nil, arg.(*setPartition).Clear(ctx)
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *Set) Listen(ctx context.Context, ch chan<- *SetEvent) error {
	_, err := util.ExecuteAllAsync(s.partitionsIter, func(arg interface{}) (interface{}, error) {
		return nil, arg.(*setPartition).Listen(ctx, ch)
	})
	if err != nil {
		return err
	}
	return nil
}
