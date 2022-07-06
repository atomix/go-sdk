// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"fmt"
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/time"
	"io"
)

var log = logging.GetLogger()

// Index is the index of an entry
type Index uint64

// Version is the version of an entry
type Version uint64

// IndexedMap is a distributed linked map
type IndexedMap[K, V any] interface {
	primitive.Primitive

	// Append appends the given key/value to the map
	Append(ctx context.Context, key K, value V) (*Entry[K, V], error)

	// Update appends the given key/value in the map
	Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error)

	// Get gets the value of the given key
	Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error)

	// GetIndex gets the entry at the given index
	GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry[K, V], error)

	// FirstIndex gets the first index in the map
	FirstIndex(ctx context.Context) (Index, error)

	// LastIndex gets the last index in the map
	LastIndex(ctx context.Context) (Index, error)

	// PrevIndex gets the index before the given index
	PrevIndex(ctx context.Context, index Index) (Index, error)

	// NextIndex gets the index after the given index
	NextIndex(ctx context.Context, index Index) (Index, error)

	// FirstEntry gets the first entry in the map
	FirstEntry(ctx context.Context) (*Entry[K, V], error)

	// LastEntry gets the last entry in the map
	LastEntry(ctx context.Context) (*Entry[K, V], error)

	// PrevEntry gets the entry before the given index
	PrevEntry(ctx context.Context, index Index) (*Entry[K, V], error)

	// NextEntry gets the entry after the given index
	NextEntry(ctx context.Context, index Index) (*Entry[K, V], error)

	// Remove removes a key from the map
	Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error)

	// RemoveIndex removes an index from the map
	RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry[K, V], error)

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

// Entry is an indexed key/value pair
type Entry[K, V any] struct {
	// Index is the unique, monotonically increasing, globally unique index of the entry. The index is static
	// for the lifetime of a key.
	Index Index

	// Key is the key of the pair
	Key K

	// Value is the value of the pair
	Value V

	// Timestamp is the entry timestamp
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

	// EventReplay indicates an entry was replayed
	EventReplay EventType = "replay"
)

// Event is a map change event
type Event[K, V any] struct {
	// Type indicates the change event type
	Type EventType

	// Entry is the event entry
	Entry Entry[K, V]
}

func New[K, V any](client indexedmapv1.IndexedMapClient) func(context.Context, string, ...Option[K, V]) (IndexedMap[K, V], error) {
	return func(ctx context.Context, name string, opts ...Option[K, V]) (IndexedMap[K, V], error) {
		var options Options[K, V]
		options.Apply(opts...)
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
		indexedMap := &indexedMapPrimitive[K, V]{
			Primitive: primitive.New(name),
			client:    client,
			keyType:   options.KeyType,
			valueType: options.ValueType,
		}
		if err := indexedMap.create(ctx, options.Tags); err != nil {
			return nil, err
		}
		return indexedMap, nil
	}
}

// indexedMapPrimitive is the default single-partition implementation of Map
type indexedMapPrimitive[K, V any] struct {
	primitive.Primitive
	client    indexedmapv1.IndexedMapClient
	keyType   generic.Type[K]
	valueType generic.Type[V]
}

func (m *indexedMapPrimitive[K, V]) Append(ctx context.Context, key K, value V) (*Entry[K, V], error) {
	keyBytes, err := m.keyType.Marshal(&key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	valueBytes, err := m.valueType.Marshal(&value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}

	request := &indexedmapv1.AppendRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
		Key: string(keyBytes),
		Value: &indexedmapv1.Value{
			Value: valueBytes,
		},
	}
	response, err := m.client.Append(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return m.newEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyType.Marshal(&key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	valueBytes, err := m.valueType.Marshal(&value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}

	request := &indexedmapv1.UpdateRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
		Key: string(keyBytes),
		Value: &indexedmapv1.Value{
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
	return m.newEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyType.Marshal(&key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	request := &indexedmapv1.GetRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
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
	return m.newEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry[K, V], error) {
	request := &indexedmapv1.GetRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
		Index: uint64(index),
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
	return m.newEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) FirstIndex(ctx context.Context) (Index, error) {
	request := &indexedmapv1.FirstEntryRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	response, err := m.client.FirstEntry(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapPrimitive[K, V]) LastIndex(ctx context.Context) (Index, error) {
	request := &indexedmapv1.LastEntryRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	response, err := m.client.LastEntry(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapPrimitive[K, V]) PrevIndex(ctx context.Context, index Index) (Index, error) {
	request := &indexedmapv1.PrevEntryRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.PrevEntry(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapPrimitive[K, V]) NextIndex(ctx context.Context, index Index) (Index, error) {
	request := &indexedmapv1.NextEntryRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.NextEntry(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapPrimitive[K, V]) FirstEntry(ctx context.Context) (*Entry[K, V], error) {
	request := &indexedmapv1.FirstEntryRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	response, err := m.client.FirstEntry(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return m.newEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) LastEntry(ctx context.Context) (*Entry[K, V], error) {
	request := &indexedmapv1.LastEntryRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	response, err := m.client.LastEntry(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return m.newEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) PrevEntry(ctx context.Context, index Index) (*Entry[K, V], error) {
	request := &indexedmapv1.PrevEntryRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.PrevEntry(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return m.newEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) NextEntry(ctx context.Context, index Index) (*Entry[K, V], error) {
	request := &indexedmapv1.NextEntryRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.NextEntry(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return m.newEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyType.Marshal(&key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}

	request := &indexedmapv1.RemoveRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
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
	return m.newEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry[K, V], error) {
	request := &indexedmapv1.RemoveRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
		Index: uint64(index),
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
	return m.newEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) Len(ctx context.Context) (int, error) {
	request := &indexedmapv1.SizeRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return int(response.Size_), nil
}

func (m *indexedMapPrimitive[K, V]) Clear(ctx context.Context) error {
	request := &indexedmapv1.ClearRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	_, err := m.client.Clear(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *indexedMapPrimitive[K, V]) Entries(ctx context.Context, ch chan<- Entry[K, V]) error {
	request := &indexedmapv1.EntriesRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
	}
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

func (m *indexedMapPrimitive[K, V]) Watch(ctx context.Context, ch chan<- Event[K, V], opts ...WatchOption) error {
	request := &indexedmapv1.EventsRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
	}
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
			case indexedmapv1.Event_INSERT:
				ch <- Event[K, V]{
					Type:  EventInsert,
					Entry: *entry,
				}
			case indexedmapv1.Event_UPDATE:
				ch <- Event[K, V]{
					Type:  EventUpdate,
					Entry: *entry,
				}
			case indexedmapv1.Event_REMOVE:
				ch <- Event[K, V]{
					Type:  EventRemove,
					Entry: *entry,
				}
			case indexedmapv1.Event_REPLAY:
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

func (m *indexedMapPrimitive[K, V]) newEntry(entry *indexedmapv1.Entry) (*Entry[K, V], error) {
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
		Index:     Index(entry.Index),
		Key:       key,
		Value:     value,
		Timestamp: time.NewTimestamp(*entry.Timestamp),
	}, nil
}

func (m *indexedMapPrimitive[K, V]) create(ctx context.Context, tags map[string]string) error {
	request := &indexedmapv1.CreateRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
		Tags: tags,
	}
	_, err := m.client.Create(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (m *indexedMapPrimitive[K, V]) Close(ctx context.Context) error {
	request := &indexedmapv1.CloseRequest{
		ID: primitivev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	_, err := m.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
