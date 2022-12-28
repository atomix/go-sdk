// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	indexedmapv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
	"github.com/atomix/go-sdk/pkg/types/scalar"
	"io"
)

var log = logging.GetLogger()

// Index is the index of an entry
type Index uint64

// IndexedMap is a distributed linked map
type IndexedMap[K scalar.Scalar, V any] interface {
	primitive.Primitive

	// Append appends the given key/value to the map
	Append(ctx context.Context, key K, value V, opts ...AppendOption) (*Entry[K, V], error)

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

// Entry is an indexed key/value pair
type Entry[K scalar.Scalar, V any] struct {
	primitive.Versioned[V]

	// Index is the unique, monotonically increasing, globally unique index of the entry. The index is static
	// for the lifetime of a key.
	Index Index

	// Key is the key of the pair
	Key K
}

func (kv *Entry[K, V]) String() string {
	return fmt.Sprintf("key: %v\nvalue: %v", kv.Key, kv.Value)
}

// Event is a map change event
type Event[K scalar.Scalar, V any] interface {
	event() *indexedmapv1.Event
}

type grpcEvent struct {
	proto *indexedmapv1.Event
}

func (e *grpcEvent) event() *indexedmapv1.Event {
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

// indexedMapPrimitive is the default single-partition implementation of Map
type indexedMapPrimitive[K scalar.Scalar, V any] struct {
	primitive.Primitive
	client     indexedmapv1.IndexedMapClient
	keyEncoder func(K) string
	keyDecoder func(string) (K, error)
	valueCodec types.Codec[V]
}

func (m *indexedMapPrimitive[K, V]) Append(ctx context.Context, key K, value V, opts ...AppendOption) (*Entry[K, V], error) {
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	request := &indexedmapv1.AppendRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Key:   m.keyEncoder(key),
		Value: valueBytes,
	}
	for i := range opts {
		opts[i].beforeAppend(request)
	}
	response, err := m.client.Append(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterAppend(response)
	}
	return m.decodeEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error) {
	valueBytes, err := m.valueCodec.Encode(value)
	if err != nil {
		return nil, errors.NewInvalid("value encoding failed", err)
	}
	request := &indexedmapv1.UpdateRequest{
		ID: runtimev1.PrimitiveID{
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
		return nil, err
	}
	for i := range opts {
		opts[i].afterUpdate(response)
	}
	return m.decodeEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	request := &indexedmapv1.GetRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Key: m.keyEncoder(key),
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	return m.decodeEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry[K, V], error) {
	request := &indexedmapv1.GetRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	return m.decodeEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) FirstIndex(ctx context.Context) (Index, error) {
	request := &indexedmapv1.FirstEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	response, err := m.client.FirstEntry(ctx, request)
	if err != nil {
		return 0, err
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapPrimitive[K, V]) LastIndex(ctx context.Context) (Index, error) {
	request := &indexedmapv1.LastEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	response, err := m.client.LastEntry(ctx, request)
	if err != nil {
		return 0, err
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapPrimitive[K, V]) PrevIndex(ctx context.Context, index Index) (Index, error) {
	request := &indexedmapv1.PrevEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.PrevEntry(ctx, request)
	if err != nil {
		return 0, err
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapPrimitive[K, V]) NextIndex(ctx context.Context, index Index) (Index, error) {
	request := &indexedmapv1.NextEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.NextEntry(ctx, request)
	if err != nil {
		return 0, err
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapPrimitive[K, V]) FirstEntry(ctx context.Context) (*Entry[K, V], error) {
	request := &indexedmapv1.FirstEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	response, err := m.client.FirstEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return m.decodeEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) LastEntry(ctx context.Context) (*Entry[K, V], error) {
	request := &indexedmapv1.LastEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	response, err := m.client.LastEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return m.decodeEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) PrevEntry(ctx context.Context, index Index) (*Entry[K, V], error) {
	request := &indexedmapv1.PrevEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.PrevEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return m.decodeEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) NextEntry(ctx context.Context, index Index) (*Entry[K, V], error) {
	request := &indexedmapv1.NextEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.NextEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return m.decodeEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	request := &indexedmapv1.RemoveRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Key: m.keyEncoder(key),
	}
	for i := range opts {
		opts[i].beforeRemove(request)
	}
	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterRemove(response)
	}
	return m.decodeEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry[K, V], error) {
	request := &indexedmapv1.RemoveRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	for i := range opts {
		opts[i].beforeRemove(request)
	}
	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterRemove(response)
	}
	return m.decodeEntry(response.Entry)
}

func (m *indexedMapPrimitive[K, V]) Len(ctx context.Context) (int, error) {
	request := &indexedmapv1.SizeRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}
	return int(response.Size_), nil
}

func (m *indexedMapPrimitive[K, V]) Clear(ctx context.Context) error {
	request := &indexedmapv1.ClearRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	_, err := m.client.Clear(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (m *indexedMapPrimitive[K, V]) List(ctx context.Context) (EntryStream[K, V], error) {
	return m.entries(ctx, false)
}

func (m *indexedMapPrimitive[K, V]) Watch(ctx context.Context) (EntryStream[K, V], error) {
	return m.entries(ctx, true)
}

func (m *indexedMapPrimitive[K, V]) entries(ctx context.Context, watch bool) (EntryStream[K, V], error) {
	request := &indexedmapv1.EntriesRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Watch: watch,
	}
	client, err := m.client.Entries(ctx, request)
	if err != nil {
		return nil, err
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

func (m *indexedMapPrimitive[K, V]) Events(ctx context.Context, opts ...EventsOption) (EventStream[K, V], error) {
	request := &indexedmapv1.EventsRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeEvents(request)
	}

	client, err := m.client.Events(ctx, request)
	if err != nil {
		return nil, err
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
			case *indexedmapv1.Event_Inserted_:
				entry, err := m.decodeKeyValue(response.Event.Key, response.Event.Index, &e.Inserted.Value)
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
			case *indexedmapv1.Event_Updated_:
				newEntry, err := m.decodeKeyValue(response.Event.Key, response.Event.Index, &e.Updated.Value)
				if err != nil {
					log.Error(err)
					continue
				}
				oldEntry, err := m.decodeKeyValue(response.Event.Key, response.Event.Index, &e.Updated.PrevValue)
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
			case *indexedmapv1.Event_Removed_:
				entry, err := m.decodeKeyValue(response.Event.Key, response.Event.Index, &e.Removed.Value)
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

func (m *indexedMapPrimitive[K, V]) decodeValue(key K, index Index, value *indexedmapv1.VersionedValue) (*Entry[K, V], error) {
	if value == nil {
		return nil, nil
	}
	decodedValue, err := m.valueCodec.Decode(value.Value)
	if err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		Versioned: primitive.Versioned[V]{
			Version: primitive.Version(value.Version),
			Value:   decodedValue,
		},
		Key:   key,
		Index: index,
	}, nil
}

func (m *indexedMapPrimitive[K, V]) decodeKeyValue(key string, index uint64, value *indexedmapv1.VersionedValue) (*Entry[K, V], error) {
	decodedKey, err := m.keyDecoder(key)
	if err != nil {
		return nil, errors.NewInvalid("key decoding failed", err)
	}
	return m.decodeValue(decodedKey, Index(index), value)
}

func (m *indexedMapPrimitive[K, V]) decodeEntry(entry *indexedmapv1.Entry) (*Entry[K, V], error) {
	return m.decodeKeyValue(entry.Key, entry.Index, entry.Value)
}

func (m *indexedMapPrimitive[K, V]) create(ctx context.Context, tags ...string) error {
	request := &indexedmapv1.CreateRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Tags: tags,
	}
	_, err := m.client.Create(ctx, request)
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (m *indexedMapPrimitive[K, V]) Close(ctx context.Context) error {
	request := &indexedmapv1.CloseRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	_, err := m.client.Close(ctx, request)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
