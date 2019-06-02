package set

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
)

type Interface interface {
	primitive.Interface
	Add(ctx context.Context, value string) (bool, error)
	Remove(ctx context.Context, value string) (bool error)
	Contains(ctx context.Context, value string) (bool, error)
	Size(ctx context.Context) (int, error)
	Clear(ctx context.Context) error
	Listen(ctx context.Context, ch chan<- *SetEvent)
}

type KeyValue struct {
	Version int64
	Key     string
	Value   []byte
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
