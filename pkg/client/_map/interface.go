package _map

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
)

type Interface interface {
	primitive.Interface
	Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*KeyValue, error)
	Get(ctx context.Context, key string, opts ...GetOption) (*KeyValue, error)
	Remove(ctx context.Context, key string, opts ...RemoveOption) (*KeyValue, error)
	Size(ctx context.Context) (int, error)
	Clear(ctx context.Context) error
	Listen(ctx context.Context, ch chan<- *MapEvent)
}

type KeyValue struct {
	Version int64
	Key     string
	Value   []byte
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
