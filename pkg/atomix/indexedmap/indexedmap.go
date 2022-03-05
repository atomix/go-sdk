// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"fmt"
	api "github.com/atomix/atomix-api/go/atomix/primitive/indexedmap"
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive/codec"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"google.golang.org/grpc"
	"io"
)

// Type is the indexed map type
const Type primitive.Type = "IndexedMap"

var log = logging.GetLogger("atomix", "client", "indexedmap")

// Index is the index of an entry
type Index uint64

// Version is the version of an entry
type Version uint64

// IndexedMap is a distributed linked map
type IndexedMap[K, V any] interface {
	primitive.Primitive

	// Append appends the given key/value to the map
	Append(ctx context.Context, key K, value V) (*Entry[K, V], error)

	// Put appends the given key/value to the map
	Put(ctx context.Context, key K, value V) (*Entry[K, V], error)

	// Set sets the given index in the map
	Set(ctx context.Context, index Index, key K, value V, opts ...SetOption) (*Entry[K, V], error)

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
	meta.ObjectMeta

	// Index is the unique, monotonically increasing, globally unique index of the entry. The index is static
	// for the lifetime of a key.
	Index Index

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

// New creates a new IndexedMap primitive
func New[K, V any](ctx context.Context, name string, conn *grpc.ClientConn, opts ...primitive.Option) (IndexedMap[K, V], error) {
	options := newIndexedMapOptions[K, V]{}
	for _, opt := range opts {
		if op, ok := opt.(Option[K, V]); ok {
			op.applyNewIndexedMap(&options)
		}
	}
	if options.keyCodec == nil {
		options.keyCodec = codec.String().(codec.Codec[K])
	}
	if options.valueCodec == nil {
		options.valueCodec = codec.Bytes().(codec.Codec[V])
	}
	m := &typedIndexedMap[K, V]{
		Client:     primitive.NewClient(Type, name, conn, opts...),
		client:     api.NewIndexedMapServiceClient(conn),
		keyCodec:   options.keyCodec,
		valueCodec: options.valueCodec,
	}
	if err := m.Create(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// typedIndexedMap is the default single-partition implementation of Map
type typedIndexedMap[K, V any] struct {
	*primitive.Client
	client     api.IndexedMapServiceClient
	keyCodec   codec.Codec[K]
	valueCodec codec.Codec[V]
}

func (m *typedIndexedMap[K, V]) Append(ctx context.Context, key K, value V) (*Entry[K, V], error) {
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
			Position: api.Position{
				Key: string(keyBytes),
			},
			Value: api.Value{
				Value: valueBytes,
			},
		},
		Preconditions: []api.Precondition{
			{
				Precondition: &api.Precondition_Metadata{
					Metadata: &metaapi.ObjectMeta{
						Type: metaapi.ObjectMeta_TOMBSTONE,
					},
				},
			},
		},
	}
	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) Put(ctx context.Context, key K, value V) (*Entry[K, V], error) {
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
			Position: api.Position{
				Key: string(keyBytes),
			},
			Value: api.Value{
				Value: valueBytes,
			},
		},
	}
	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) Set(ctx context.Context, index Index, key K, value V, opts ...SetOption) (*Entry[K, V], error) {
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
			Position: api.Position{
				Index: uint64(index),
				Key:   string(keyBytes),
			},
			Value: api.Value{
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
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}
	request := &api.GetRequest{
		Headers: m.GetHeaders(),
		Position: api.Position{
			Key: string(keyBytes),
		},
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
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry[K, V], error) {
	request := &api.GetRequest{
		Headers: m.GetHeaders(),
		Position: api.Position{
			Index: uint64(index),
		},
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
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) FirstIndex(ctx context.Context) (Index, error) {
	request := &api.FirstEntryRequest{
		Headers: m.GetHeaders(),
	}
	response, err := m.client.FirstEntry(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *typedIndexedMap[K, V]) LastIndex(ctx context.Context) (Index, error) {
	request := &api.LastEntryRequest{
		Headers: m.GetHeaders(),
	}
	response, err := m.client.LastEntry(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *typedIndexedMap[K, V]) PrevIndex(ctx context.Context, index Index) (Index, error) {
	request := &api.PrevEntryRequest{
		Headers: m.GetHeaders(),
		Index:   uint64(index),
	}
	response, err := m.client.PrevEntry(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *typedIndexedMap[K, V]) NextIndex(ctx context.Context, index Index) (Index, error) {
	request := &api.NextEntryRequest{
		Headers: m.GetHeaders(),
		Index:   uint64(index),
	}
	response, err := m.client.NextEntry(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *typedIndexedMap[K, V]) FirstEntry(ctx context.Context) (*Entry[K, V], error) {
	request := &api.FirstEntryRequest{
		Headers: m.GetHeaders(),
	}
	response, err := m.client.FirstEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) LastEntry(ctx context.Context) (*Entry[K, V], error) {
	request := &api.LastEntryRequest{
		Headers: m.GetHeaders(),
	}
	response, err := m.client.LastEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) PrevEntry(ctx context.Context, index Index) (*Entry[K, V], error) {
	request := &api.PrevEntryRequest{
		Headers: m.GetHeaders(),
		Index:   uint64(index),
	}
	response, err := m.client.PrevEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) NextEntry(ctx context.Context, index Index) (*Entry[K, V], error) {
	request := &api.NextEntryRequest{
		Headers: m.GetHeaders(),
		Index:   uint64(index),
	}
	response, err := m.client.NextEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	keyBytes, err := m.keyCodec.Encode(key)
	if err != nil {
		return nil, errors.NewInvalid("key encoding failed", err)
	}

	request := &api.RemoveRequest{
		Headers: m.GetHeaders(),
		Entry: &api.Entry{
			Position: api.Position{
				Key: string(keyBytes),
			},
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
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry[K, V], error) {
	request := &api.RemoveRequest{
		Headers: m.GetHeaders(),
		Entry: &api.Entry{
			Position: api.Position{
				Index: uint64(index),
			},
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
	return m.newEntry(response.Entry)
}

func (m *typedIndexedMap[K, V]) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Headers: m.GetHeaders(),
	}
	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return int(response.Size_), nil
}

func (m *typedIndexedMap[K, V]) Clear(ctx context.Context) error {
	request := &api.ClearRequest{
		Headers: m.GetHeaders(),
	}
	_, err := m.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (m *typedIndexedMap[K, V]) Entries(ctx context.Context, ch chan<- Entry[K, V]) error {
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

func (m *typedIndexedMap[K, V]) Watch(ctx context.Context, ch chan<- Event[K, V], opts ...WatchOption) error {
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

func (m *typedIndexedMap[K, V]) newEntry(entry *api.Entry) (*Entry[K, V], error) {
	if entry == nil {
		return nil, nil
	}
	key, err := m.keyCodec.Decode([]byte(entry.Key))
	if err != nil {
		return nil, errors.NewInvalid("key decoding failed", err)
	}
	value, err := m.valueCodec.Decode(entry.Value.Value)
	if err != nil {
		return nil, errors.NewInvalid("value decoding failed", err)
	}
	return &Entry[K, V]{
		ObjectMeta: meta.FromProto(entry.Value.ObjectMeta),
		Index:      Index(entry.Index),
		Key:        key,
		Value:      value,
	}, nil
}
