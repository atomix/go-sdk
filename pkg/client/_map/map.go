package _map

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"google.golang.org/grpc"
)

func NewMap(namespace string, name string, partitions []*grpc.ClientConn, opts ...session.Option) (*Map, error) {
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

	mapPartitions := make([]*mapPartition, len(results))
	for i, result := range results {
		mapPartitions[i] = result.(*mapPartition)
	}

	return &Map{
		Namespace:      namespace,
		Name:           name,
		partitions:     mapPartitions,
		partitionsIter: results,
	}, nil
}

type Map struct {
	Interface
	Namespace      string
	Name           string
	partitions     []*mapPartition
	partitionsIter []interface{}
}

func (m *Map) getPartition(key string) (*mapPartition, error) {
	i, err := util.GetPartitionIndex(key, len(m.partitions))
	if err != nil {
		return nil, err
	}
	return m.partitions[i], nil
}

func (m *Map) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*KeyValue, error) {
	session, err := m.getPartition(key)
	if err != nil {
		return nil, err
	}
	return session.Put(ctx, key, value, opts...)
}

func (m *Map) Get(ctx context.Context, key string, opts ...GetOption) (*KeyValue, error) {
	session, err := m.getPartition(key)
	if err != nil {
		return nil, err
	}
	return session.Get(ctx, key, opts...)
}

func (m *Map) Remove(ctx context.Context, key string, opts ...RemoveOption) (*KeyValue, error) {
	session, err := m.getPartition(key)
	if err != nil {
		return nil, err
	}
	return session.Remove(ctx, key, opts...)
}

func (m *Map) Size(ctx context.Context) (int, error) {
	results, err := util.ExecuteAllAsync(m.partitionsIter, func(arg interface{}) (interface{}, error) {
		return arg.(*mapPartition).Size(ctx)
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

func (m *Map) Clear(ctx context.Context) error {
	_, err := util.ExecuteAllAsync(m.partitionsIter, func(arg interface{}) (interface{}, error) {
		return nil, arg.(*mapPartition).Clear(ctx)
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *Map) Listen(ctx context.Context, ch chan<- *MapEvent) error {
	_, err := util.ExecuteAllAsync(m.partitionsIter, func(arg interface{}) (interface{}, error) {
		return nil, arg.(*mapPartition).Listen(ctx, ch)
	})
	if err != nil {
		return err
	}
	return nil
}
