package map_

import (
	"context"
	"fmt"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"google.golang.org/grpc"
	"sync"
)

type MapClient interface {
	GetMap(ctx context.Context, name string, opts ...session.SessionOption) (Map, error)
}

type Map interface {
	primitive.Primitive
	Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*KeyValue, error)
	Get(ctx context.Context, key string, opts ...GetOption) (*KeyValue, error)
	Remove(ctx context.Context, key string, opts ...RemoveOption) (*KeyValue, error)
	Size(ctx context.Context) (int, error)
	Clear(ctx context.Context) error
	Entries(ctx context.Context, ch chan<- *KeyValue) error
	Listen(ctx context.Context, ch chan<- *MapEvent) error
}

type KeyValue struct {
	Version int64
	Key     string
	Value   []byte
}

func (kv KeyValue) String() string {
	return fmt.Sprintf("key: %s\nvalue: %s\nversion: %d", kv.Key, string(kv.Value), kv.Version)
}

type MapEventType string

const (
	EVENT_INSERTED MapEventType = "inserted"
	EVENT_UPDATED  MapEventType = "updated"
	EVENT_REMOVED  MapEventType = "removed"
)

type MapEvent struct {
	Type    MapEventType
	Key     string
	Value   []byte
	Version int64
}

func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.SessionOption) (Map, error) {
	results, err := util.ExecuteOrderedAsync(len(partitions), func(i int) (interface{}, error) {
		return newPartition(ctx, partitions[i], name, opts...)
	})
	if err != nil {
		return nil, err
	}

	maps := make([]Map, len(results))
	for i, result := range results {
		maps[i] = result.(Map)
	}

	return &_map{
		name:       name,
		partitions: maps,
	}, nil
}

type _map struct {
	name       primitive.Name
	partitions []Map
}

func (m *_map) Name() primitive.Name {
	return m.name
}

func (m *_map) getPartition(key string) (Map, error) {
	i, err := util.GetPartitionIndex(key, len(m.partitions))
	if err != nil {
		return nil, err
	}
	return m.partitions[i], nil
}

func (m *_map) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*KeyValue, error) {
	session, err := m.getPartition(key)
	if err != nil {
		return nil, err
	}
	return session.Put(ctx, key, value, opts...)
}

func (m *_map) Get(ctx context.Context, key string, opts ...GetOption) (*KeyValue, error) {
	session, err := m.getPartition(key)
	if err != nil {
		return nil, err
	}
	return session.Get(ctx, key, opts...)
}

func (m *_map) Remove(ctx context.Context, key string, opts ...RemoveOption) (*KeyValue, error) {
	session, err := m.getPartition(key)
	if err != nil {
		return nil, err
	}
	return session.Remove(ctx, key, opts...)
}

func (m *_map) Size(ctx context.Context) (int, error) {
	results, err := util.ExecuteAsync(len(m.partitions), func(i int) (interface{}, error) {
		return m.partitions[i].Size(ctx)
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

func (m *_map) Entries(ctx context.Context, ch chan<- *KeyValue) error {
	n := len(m.partitions)
	wg := sync.WaitGroup{}
	wg.Add(n)

	go func() {
		wg.Wait()
		close(ch)
	}()

	return util.IterAsync(n, func(i int) error {
		c := make(chan *KeyValue)
		go func() {
			for kv := range c {
				ch <- kv
			}
			wg.Done()
		}()
		return m.partitions[i].Entries(ctx, c)
	})
}

func (m *_map) Clear(ctx context.Context) error {
	return util.IterAsync(len(m.partitions), func(i int) error {
		return m.partitions[i].Clear(ctx)
	})
}

func (m *_map) Listen(ctx context.Context, ch chan<- *MapEvent) error {
	return util.IterAsync(len(m.partitions), func(i int) error {
		return m.partitions[i].Listen(ctx, ch)
	})
}

func (m *_map) Close() error {
	return util.IterAsync(len(m.partitions), func(i int) error {
		return m.partitions[i].Close()
	})
}

func (m *_map) Delete() error {
	return util.IterAsync(len(m.partitions), func(i int) error {
		return m.partitions[i].Delete()
	})
}
