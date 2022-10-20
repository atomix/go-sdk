// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	"fmt"
	"github.com/atomix/go-client/pkg/generic"
	"github.com/atomix/go-client/pkg/generic/scalar"
	"github.com/atomix/go-client/pkg/primitive"
	"github.com/atomix/go-client/pkg/stream"
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"io"
)

var log = logging.GetLogger()

// Map is a distributed set of keys and values
type Map[K scalar.Scalar, V any] interface {
	primitive.Primitive

	// Put sets a key/value pair in the map
	Put(ctx context.Context, key K, value V, opts ...PutOption) (*Entry[K, V], error)

	// Insert sets a key/value pair in the map
	Insert(ctx context.Context, key K, value V, opts ...InsertOption) (*Entry[K, V], error)

	// Update sets a key/value pair in the map
	Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error)

	// Get gets the value of the given key
	Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error)

	// Remove removes a key from the map
	Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error)

	// Len returns the number of entries in the map
	Len(ctx context.Context) (int, error)

	// Clear removes all entries from the map
	Clear(ctx context.Context) error

	// List lists the entries in the map
	// This is a non-blocking method. If the method returns without error, key/value paids will be pushed on to the
	// given channel and the channel will be closed once all entries have been read from the map.
	List(ctx context.Context) (EntryStream[K, V], error)

	// Watch watches the map for changes
	// This is a non-blocking method. If the method returns without error, map events will be pushed onto
	// the given channel in the order in which they occur.
	Watch(ctx context.Context) (EntryStream[K, V], error)

	// Events watches the map for change events
	// This is a non-blocking method. If the method returns without error, map events will be pushed onto
	// the given channel in the order in which they occur.
	Events(ctx context.Context, opts ...EventsOption) (EventStream[K, V], error)
}

type EntryStream[K scalar.Scalar, V any] stream.Stream[*Entry[K, V]]

type EventStream[K scalar.Scalar, V any] stream.Stream[Event[K, V]]

// Entry is a versioned key/value pair
type Entry[K scalar.Scalar, V any] struct {
	primitive.Versioned[V]

	// Key is the key of the pair
	Key K
}

func (kv *Entry[K, V]) String() string {
	return fmt.Sprintf("key: %v\nvalue: %v", kv.Key, kv.Value)
}

// Event is a map change event
type Event[K scalar.Scalar, V any] interface {
	event() *mapv1.Event
}

type grpcEvent struct {
	proto *mapv1.Event
}

func (e *grpcEvent) event() *mapv1.Event {
	return e.proto
}

type Inserted[K scalar.Scalar, V any] struct {
	*grpcEvent
	Entry *Entry[K, V]
}

type Updated[K scalar.Scalar, V any] struct {
	*grpcEvent
	NewEntry *Entry[K, V]
	OldEntry *Entry[K, V]
}

type Removed[K scalar.Scalar, V any] struct {
	*grpcEvent
	Entry   *Entry[K, V]
	Expired bool
}

type atomicMapPrimitive[K scalar.Scalar, V any] struct {
	primitive.Primitive
	client     mapv1.MapClient
	keyEncoder func(K) string
	keyDecoder func(string) (K, error)
	valueCodec generic.Codec[V]
}

func (m *atomicMapPrimitive[K, V]) Put(ctx context.Context, key K, value V, opts ...PutOption) (*Entry[K, V], error) {
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	request := &mapv1.PutRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Key:   m.keyEncoder(key),
		Value: valueBytes,
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
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Value:   value,
			Version: primitive.Version(response.Version),
		},
		Key: key,
	}, nil
}

func (m *atomicMapPrimitive[K, V]) Insert(ctx context.Context, key K, value V, opts ...InsertOption) (*Entry[K, V], error) {
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	request := &mapv1.InsertRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Key:   m.keyEncoder(key),
		Value: valueBytes,
	}
	for i := range opts {
		opts[i].beforeInsert(request)
	}
	response, err := m.client.Insert(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterInsert(response)
	}
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Value:   value,
			Version: primitive.Version(response.Version),
		},
		Key: key,
	}, nil
}

func (m *atomicMapPrimitive[K, V]) Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error) {
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	request := &mapv1.UpdateRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Key:   m.keyEncoder(key),
		Value: valueBytes,
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
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Value:   value,
			Version: primitive.Version(response.Version),
		},
		Key: key,
	}, nil
}

func (m *atomicMapPrimitive[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	request := &mapv1.GetRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Key: m.keyEncoder(key),
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
	return m.decodeValue(key, &response.Value)
}

func (m *atomicMapPrimitive[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	request := &mapv1.RemoveRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Key: m.keyEncoder(key),
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
	return m.decodeValue(key, &response.Value)
}

func (m *atomicMapPrimitive[K, V]) Len(ctx context.Context) (int, error) {
	request := &mapv1.SizeRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return int(response.Size_), nil
}

func (m *atomicMapPrimitive[K, V]) Clear(ctx context.Context) error {
	request := &mapv1.ClearRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	_, err := m.client.Clear(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *atomicMapPrimitive[K, V]) List(ctx context.Context) (EntryStream[K, V], error) {
	return m.entries(ctx, false)
}

func (m *atomicMapPrimitive[K, V]) Watch(ctx context.Context) (EntryStream[K, V], error) {
	return m.entries(ctx, true)
}

func (m *atomicMapPrimitive[K, V]) entries(ctx context.Context, watch bool) (EntryStream[K, V], error) {
	request := &mapv1.EntriesRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Watch: watch,
	}
	client, err := m.client.Entries(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}

	ch := make(chan stream.Result[*Entry[K, V]])
	go func() {
		defer close(ch)
		for {
			response, err := client.Recv()
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
			entry, err := m.decodeEntry(&response.Entry)
			if err != nil {
				log.Error(err)
			} else {
				ch <- stream.Result[*Entry[K, V]]{
					Value: entry,
				}
			}
		}
	}()
	return stream.NewChannelStream[*Entry[K, V]](ch), nil
}

func (m *atomicMapPrimitive[K, V]) Events(ctx context.Context, opts ...EventsOption) (EventStream[K, V], error) {
	request := &mapv1.EventsRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeEvents(request)
	}

	client, err := m.client.Events(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}

	ch := make(chan stream.Result[Event[K, V]])
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
			response, err := client.Recv()
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
				opts[i].afterEvents(response)
			}

			switch e := response.Event.Event.(type) {
			case *mapv1.Event_Inserted_:
				entry, err := m.decodeKeyValue(response.Event.Key, &e.Inserted.Value)
				if err != nil {
					log.Error(err)
					continue
				}

				ch <- stream.Result[Event[K, V]]{
					Value: &Inserted[K, V]{
						grpcEvent: &grpcEvent{&response.Event},
						Entry:     entry,
					},
				}
			case *mapv1.Event_Updated_:
				newEntry, err := m.decodeKeyValue(response.Event.Key, &e.Updated.Value)
				if err != nil {
					log.Error(err)
					continue
				}
				oldEntry, err := m.decodeKeyValue(response.Event.Key, &e.Updated.PrevValue)
				if err != nil {
					log.Error(err)
					continue
				}

				ch <- stream.Result[Event[K, V]]{
					Value: &Updated[K, V]{
						grpcEvent: &grpcEvent{&response.Event},
						NewEntry:  newEntry,
						OldEntry:  oldEntry,
					},
				}
			case *mapv1.Event_Removed_:
				entry, err := m.decodeKeyValue(response.Event.Key, &e.Removed.Value)
				if err != nil {
					log.Error(err)
					continue
				}

				ch <- stream.Result[Event[K, V]]{
					Value: &Removed[K, V]{
						grpcEvent: &grpcEvent{&response.Event},
						Entry:     entry,
						Expired:   e.Removed.Expired,
					},
				}
			}
		}
	}()

	select {
	case <-openCh:
		return stream.NewChannelStream[Event[K, V]](ch), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *atomicMapPrimitive[K, V]) decodeValue(key K, value *mapv1.VersionedValue) (*Entry[K, V], error) {
	if value == nil {
		return &Entry[K, V]{
			Key: key,
		}, nil
	}
	decodedValue, err := m.valueCodec.Decode(value.Value)
	if err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Value:   decodedValue,
			Version: primitive.Version(value.Version),
		},
		Key: key,
	}, nil
}

func (m *atomicMapPrimitive[K, V]) decodeKeyValue(key string, value *mapv1.VersionedValue) (*Entry[K, V], error) {
	decodedKey, err := m.keyDecoder(key)
	if err != nil {
		return nil, errors.NewInvalid("key decoding failed", err)
	}
	return m.decodeValue(decodedKey, value)
}

func (m *atomicMapPrimitive[K, V]) decodeEntry(entry *mapv1.Entry) (*Entry[K, V], error) {
	return m.decodeKeyValue(entry.Key, entry.Value)
}

func (m *atomicMapPrimitive[K, V]) create(ctx context.Context, tags ...string) error {
	request := &mapv1.CreateRequest{
		ID: runtimev1.PrimitiveId{
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

func (m *atomicMapPrimitive[K, V]) Close(ctx context.Context) error {
	request := &mapv1.CloseRequest{
		ID: runtimev1.PrimitiveId{
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
