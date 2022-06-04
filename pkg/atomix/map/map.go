// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	"fmt"
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/time"
	"io"
)

var log = logging.GetLogger()

// Map is a distributed set of keys and values
type Map[K, V any] interface {
	primitive.Primitive

	// Put sets a key/value pair in the map
	Put(ctx context.Context, key K, value V, opts ...PutOption) (*Entry[K, V], error)

	// Insert inserts a key/value pair into the map
	Insert(ctx context.Context, key K, value V) (*Entry[K, V], error)

	// Update updates a key/value pair in the map
	Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error)

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
	// Key is the key of the pair
	Key K

	// Value is the value of the pair
	Value V

	// Timestamp is the entry creation timestamp
	Timestamp time.Timestamp
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

func New[K, V any](client mapv1.MapClient) func(context.Context, primitive.ID, ...Option[K, V]) (Map[K, V], error) {
	return func(ctx context.Context, id primitive.ID, opts ...Option[K, V]) (Map[K, V], error) {
		var options Options[K, V]
		options.apply(opts...)
		if options.KeyType == nil {
			stringType := generic.String()
			if keyType, ok := stringType.(generic.Type[K]); ok {
				options.KeyType = keyType
			} else {
				return nil, errors.NewInvalid("must configure a generic type for key parameter")
			}
		}
		if options.ValueType == nil {
			jsonType := generic.JSON[V]()
			if valueType, ok := jsonType.(generic.Type[V]); ok {
				options.ValueType = valueType
			} else {
				return nil, errors.NewInvalid("must configure a generic type for value parameter")
			}
		}
		indexedMap := &mapPrimitive[K, V]{
			Primitive: primitive.New(id),
			client:    client,
			keyType:   options.KeyType,
			valueType: options.ValueType,
		}
		if err := indexedMap.create(ctx); err != nil {
			return nil, err
		}
		return indexedMap, nil
	}
}

type mapPrimitive[K, V any] struct {
	primitive.Primitive
	client    mapv1.MapClient
	keyType   generic.Type[K]
	valueType generic.Type[V]
}

func (m *mapPrimitive[K, V]) Put(ctx context.Context, key K, value V, opts ...PutOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyType.Marshal(&key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	valueBytes, err := m.valueType.Marshal(&value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}

	request := &mapv1.PutRequest{
		Key: string(keyBytes),
		Value: &mapv1.Value{
			Value: valueBytes,
		},
	}
	for i := range opts {
		opts[i].beforePut(request)
	}
	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterPut(response)
	}
	return m.newEntry(&response.Entry)
}

func (m *mapPrimitive[K, V]) Insert(ctx context.Context, key K, value V) (*Entry[K, V], error) {
	keyBytes, err := m.keyType.Marshal(&key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	valueBytes, err := m.valueType.Marshal(&value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}

	request := &mapv1.InsertRequest{
		Key: string(keyBytes),
		Value: &mapv1.Value{
			Value: valueBytes,
		},
	}
	response, err := m.client.Insert(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return m.newEntry(&response.Entry)
}

func (m *mapPrimitive[K, V]) Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyType.Marshal(&key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	valueBytes, err := m.valueType.Marshal(&value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}

	request := &mapv1.UpdateRequest{
		Key: string(keyBytes),
		Value: &mapv1.Value{
			Value: valueBytes,
		},
	}
	for i := range opts {
		opts[i].beforeUpdate(request)
	}
	response, err := m.client.Update(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterUpdate(response)
	}
	return m.newEntry(&response.Entry)
}

func (m *mapPrimitive[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyType.Marshal(&key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	request := &mapv1.GetRequest{
		Key: string(keyBytes),
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	return m.newEntry(&response.Entry)
}

func (m *mapPrimitive[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyType.Marshal(&key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	request := &mapv1.RemoveRequest{
		Key: string(keyBytes),
	}
	for i := range opts {
		opts[i].beforeRemove(request)
	}
	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterRemove(response)
	}
	return m.newEntry(&response.Entry)
}

func (m *mapPrimitive[K, V]) Len(ctx context.Context) (int, error) {
	request := &mapv1.SizeRequest{}
	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return int(response.Size_), nil
}

func (m *mapPrimitive[K, V]) Clear(ctx context.Context) error {
	request := &mapv1.ClearRequest{}
	_, err := m.client.Clear(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *mapPrimitive[K, V]) Entries(ctx context.Context, ch chan<- Entry[K, V]) error {
	request := &mapv1.EntriesRequest{}
	stream, err := m.client.Entries(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}

	go func() {
		defer close(ch)
		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				err = errors.FromProto(err)
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

func (m *mapPrimitive[K, V]) Watch(ctx context.Context, ch chan<- Event[K, V], opts ...WatchOption) error {
	request := &mapv1.EventsRequest{}
	for i := range opts {
		opts[i].beforeWatch(request)
	}

	stream, err := m.client.Events(ctx, request)
	if err != nil {
		return errors.FromProto(err)
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
				err = errors.FromProto(err)
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
			case mapv1.Event_INSERT:
				ch <- Event[K, V]{
					Type:  EventInsert,
					Entry: *entry,
				}
			case mapv1.Event_UPDATE:
				ch <- Event[K, V]{
					Type:  EventUpdate,
					Entry: *entry,
				}
			case mapv1.Event_REMOVE:
				ch <- Event[K, V]{
					Type:  EventRemove,
					Entry: *entry,
				}
			case mapv1.Event_REPLAY:
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

func (m *mapPrimitive[K, V]) newEntry(entry *mapv1.Entry) (*Entry[K, V], error) {
	if entry == nil {
		return nil, nil
	}
	var key K
	if err := m.keyType.Unmarshal([]byte(entry.Key), &key); err != nil {
		return nil, errors.NewInvalid("key decoding failed", err)
	}
	var value V
	if err := m.valueType.Unmarshal(entry.Value.Value, &value); err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		Key:       key,
		Value:     value,
		Timestamp: time.NewTimestamp(*entry.Timestamp),
	}, nil
}

func (m *mapPrimitive[K, V]) create(ctx context.Context) error {
	request := &mapv1.CreateRequest{
		Config: mapv1.MapConfig{},
	}
	ctx = primitive.AppendToOutgoingContext(ctx, m.ID())
	_, err := m.client.Create(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (m *mapPrimitive[K, V]) Close(ctx context.Context) error {
	request := &mapv1.CloseRequest{}
	ctx = primitive.AppendToOutgoingContext(ctx, m.ID())
	_, err := m.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
