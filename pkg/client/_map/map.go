package _map

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/partition"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"hash/fnv"
	"sort"
)

func NewMap(namespace string, name string, partitions []*partition.Partition, opts ...session.Option) (*Map, error) {
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Id < partitions[i].Id
	})
	mapPartitions := make([]*Session, len(partitions))
	for i, partition := range partitions {
		mapPartition, err := newSession(partition.Conn, namespace, name, opts...)
		if err != nil {
			return nil, err
		}
		mapPartitions[i] = mapPartition
	}
	return &Map{
		Namespace:  namespace,
		Name:       name,
		partitions: mapPartitions,
	}, nil
}

type Map struct {
	mapInterface
	Namespace  string
	Name       string
	partitions []*Session
}

func (m *Map) getPartition(key string) (*Session, error) {
	h := fnv.New32a()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	i := h.Sum32() % uint32(len(m.partitions))
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
	size := 0
	for _, partition := range m.partitions {
		s, err := partition.Size(ctx)
		if err != nil {
			return 0, err
		}
		size += s
	}
	return size, nil
}

func (m *Map) Clear(ctx context.Context) error {
	for _, partition := range m.partitions {
		if err := partition.Clear(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *Map) Listen(ctx context.Context, ch chan<- *MapEvent) error {
	for _, partition := range m.partitions {
		if err := partition.Listen(ctx, ch); err != nil {
			return err
		}
	}
	return nil
}
