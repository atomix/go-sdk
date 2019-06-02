package _map

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"google.golang.org/grpc"
	"hash/fnv"
	"sync"
)

func NewMap(namespace string, name string, partitions []*grpc.ClientConn, opts ...session.Option) (*Map, error) {
	mapPartitions := make([]*Session, len(partitions))
	for i, conn := range partitions {
		mapPartition, err := newSession(conn, namespace, name, opts...)
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
	wg := sync.WaitGroup{}
	results := make(chan int, len(m.partitions))
	errors := make(chan error, len(m.partitions))

	for _, partition := range m.partitions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := partition.Size(ctx)
			if err != nil {
				errors <- err
			} else {
				results <- result
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
		close(errors)
	}()

	for err := range errors {
		return 0, err
	}

	size := 0
	for result := range results {
		size += result
	}
	return size, nil
}

func (m *Map) Clear(ctx context.Context) error {
	wg := sync.WaitGroup{}
	errors := make(chan error, len(m.partitions))

	for _, partition := range m.partitions {
		wg.Add(1)
		go func() {
			if err := partition.Clear(ctx); err != nil {
				errors <- err
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errors)
	}()

	for err := range errors {
		return err
	}

	return nil
}

func (m *Map) Listen(ctx context.Context, ch chan<- *MapEvent) error {
	wg := sync.WaitGroup{}
	errors := make(chan error, len(m.partitions))

	for _, partition := range m.partitions {
		wg.Add(1)
		go func() {
			if err := partition.Listen(ctx, ch); err != nil {
				errors <- err
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errors)
	}()

	for err := range errors {
		return err
	}

	return nil
}
