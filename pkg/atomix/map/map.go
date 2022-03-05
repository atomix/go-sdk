// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	"fmt"
	api "github.com/atomix/atomix-api/go/atomix/primitive/map"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive/codec"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"google.golang.org/grpc"
	"io"
)

const Type primitive.Type = "Map"

var log = logging.GetLogger("atomix", "client", "map")

// Map is a distributed set of keys and values
type Map[K, V any] interface {
	primitive.Primitive

	// Put sets a key/value pair in the map
	Put(ctx context.Context, key K, value V, opts ...PutOption) (*Entry[K, V], error)

	// Get gets the value of the given key
	Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error)

	// Remove removes a key from the map
	Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error)

	// Len returns the number of entries in the map
	Len(ctx context.Context) (int, error)

	// Clear removes all entries from the map
	Clear(ctx context.Context) error

	// Entries lists the entries in the map
	// This is a non-blocking method. If the method returns without error, key/value paids will be pushed on to the
	// given channel and the channel will be closed once all entries have been read from the map.
	Entries(ctx context.Context, ch chan<- Entry[K, V]) error

	// Watch watches the map for changes
	// This is a non-blocking method. If the method returns without error, map events will be pushed onto
	// the given channel in the order in which they occur.
	Watch(ctx context.Context, ch chan<- Event[K, V], opts ...WatchOption) error
}

// Version is an entry version
type Version uint64

// Entry is a versioned key/value pair
type Entry[K, V any] struct {
	meta.ObjectMeta

	// Key is the key of the pair
	Key K

	// Value is the value of the pair
	Value V
}

func (kv Entry[K, V]) String() string {
	return fmt.Sprintf("key: %v\nvalue: %v", kv.Key, kv.Value)
}

// EventType is the type of a map event
type EventType string

const (
	// EventInsert indicates a key was newly created in the map
	EventInsert EventType = "insert"

	// EventUpdate indicates the value of an existing key was changed
	EventUpdate EventType = "update"

	// EventRemove indicates a key was removed from the map
	EventRemove EventType = "remove"

	// EventReplay indicates a key was replayed
	EventReplay EventType = "replay"
)

// Event is a map change event
type Event[K, V any] struct {
	// Type indicates the change event type
	Type EventType

	// Entry is the event entry
	Entry Entry[K, V]
}

// New creates a new partitioned Map
func New[K, V any](ctx context.Context, name string, conn *grpc.ClientConn, opts ...primitive.Option) (Map[K, V], error) {
	mapOptions := newMapOptions[K, V]{}
	for _, opt := range opts {
		if op, ok := opt.(Option[K, V]); ok {
			op.applyNewMap(&mapOptions)
		}
	}
	if mapOptions.keyCodec == nil {
		mapOptions.keyCodec = codec.String().(codec.Codec[K])
	}
	if mapOptions.valueCodec == nil {
		mapOptions.valueCodec = codec.Bytes().(codec.Codec[V])
	}
	m := &typedMap[K, V]{
		Client:     primitive.NewClient(Type, name, conn, opts...),
		client:     api.NewMapServiceClient(conn),
		keyCodec:   mapOptions.keyCodec,
		valueCodec: mapOptions.valueCodec,
	}
	if err := m.Create(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

type typedMap[K, V any] struct {
	*primitive.Client
	client     api.MapServiceClient
	keyCodec   codec.Codec[K]
	valueCodec codec.Codec[V]
}

func (m *typedMap[K, V]) Put(ctx context.Context, key K, value V, opts ...PutOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}

	request := &api.PutRequest{
		Headers: m.GetHeaders(),
		Entry: api.Entry{
			Key: api.Key{
				Key: string(keyBytes),
			},
			Value: &api.Value{
				Value: valueBytes,
			},
		},
	}
	for i := range opts {
		opts[i].beforePut(request)
	}
	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	for i := range opts {
		opts[i].afterPut(response)
	}
	return m.newEntry(&response.Entry)
}

func (m *typedMap[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	request := &api.GetRequest{
		Headers: m.GetHeaders(),
		Key:     string(keyBytes),
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	return m.newEntry(&response.Entry)
}

func (m *typedMap[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	request := &api.RemoveRequest{
		Headers: m.GetHeaders(),
		Key: api.Key{
			Key: string(keyBytes),
		},
	}
	for i := range opts {
		opts[i].beforeRemove(request)
	}
	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	for i := range opts {
		opts[i].afterRemove(response)
	}
	return m.newEntry(&response.Entry)
}

func (m *typedMap[K, V]) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Headers: m.GetHeaders(),
	}
	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return int(response.Size_), nil
}

func (m *typedMap[K, V]) Clear(ctx context.Context) error {
	request := &api.ClearRequest{
		Headers: m.GetHeaders(),
	}
	_, err := m.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (m *typedMap[K, V]) Entries(ctx context.Context, ch chan<- Entry[K, V]) error {
	request := &api.EntriesRequest{
		Headers: m.GetHeaders(),
	}
	stream, err := m.client.Entries(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	go func() {
		defer close(ch)
		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				err = errors.From(err)
				if errors.IsCanceled(err) || errors.IsTimeout(err) {
					return
				}
				log.Errorf("Entries failed: %v", err)
				return
			}
			entry, err := m.newEntry(&response.Entry)
			if err != nil {
				log.Error(err)
			} else {
				ch <- *entry
			}
		}
	}()
	return nil
}

func (m *typedMap[K, V]) Watch(ctx context.Context, ch chan<- Event[K, V], opts ...WatchOption) error {
	request := &api.EventsRequest{
		Headers: m.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeWatch(request)
	}

	stream, err := m.client.Events(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	openCh := make(chan struct{})
	go func() {
		defer close(ch)
		open := false
		defer func() {
			if !open {
				close(openCh)
			}
		}()
		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				err = errors.From(err)
				if errors.IsCanceled(err) || errors.IsTimeout(err) {
					return
				}
				log.Errorf("Watch failed: %v", err)
				return
			}

			if !open {
				close(openCh)
				open = true
			}

			for i := range opts {
				opts[i].afterWatch(response)
			}

			entry, err := m.newEntry(&response.Event.Entry)
			if err != nil {
				log.Error(err)
				continue
			}

			switch response.Event.Type {
			case api.Event_INSERT:
				ch <- Event[K, V]{
					Type:  EventInsert,
					Entry: *entry,
				}
			case api.Event_UPDATE:
				ch <- Event[K, V]{
					Type:  EventUpdate,
					Entry: *entry,
				}
			case api.Event_REMOVE:
				ch <- Event[K, V]{
					Type:  EventRemove,
					Entry: *entry,
				}
			case api.Event_REPLAY:
				ch <- Event[K, V]{
					Type:  EventReplay,
					Entry: *entry,
				}
			}
		}
	}()

	select {
	case <-openCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *typedMap[K, V]) newEntry(entry *api.Entry) (*Entry[K, V], error) {
	if entry == nil {
		return nil, nil
	}
	key, err := m.keyCodec.Decode([]byte(entry.Key.Key))
	if err != nil {
		return nil, errors.NewInvalid("key decoding failed", err)
	}
	value, err := m.valueCodec.Decode(entry.Value.Value)
	if err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		ObjectMeta: meta.FromProto(entry.Key.ObjectMeta),
		Key:        key,
		Value:      value,
	}, nil
}
