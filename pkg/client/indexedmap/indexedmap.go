// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexedmap

import (
	"context"
	"fmt"
	api "github.com/atomix/api/go/atomix/primitive/indexedmap"
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/meta"
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

// Client provides an API for creating IndexedMaps
type Client interface {
	// GetIndexedMap gets the IndexedMap instance of the given name
	GetIndexedMap(ctx context.Context, name string, opts ...Option) (IndexedMap, error)
}

// IndexedMap is a distributed linked map
type IndexedMap interface {
	primitive.Primitive

	// Append appends the given key/value to the map
	Append(ctx context.Context, key string, value []byte) (*Entry, error)

	// Put appends the given key/value to the map
	Put(ctx context.Context, key string, value []byte) (*Entry, error)

	// Set sets the given index in the map
	Set(ctx context.Context, index Index, key string, value []byte, opts ...SetOption) (*Entry, error)

	// Get gets the value of the given key
	Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error)

	// GetIndex gets the entry at the given index
	GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry, error)

	// FirstIndex gets the first index in the map
	FirstIndex(ctx context.Context) (Index, error)

	// LastIndex gets the last index in the map
	LastIndex(ctx context.Context) (Index, error)

	// PrevIndex gets the index before the given index
	PrevIndex(ctx context.Context, index Index) (Index, error)

	// NextIndex gets the index after the given index
	NextIndex(ctx context.Context, index Index) (Index, error)

	// FirstEntry gets the first entry in the map
	FirstEntry(ctx context.Context) (*Entry, error)

	// LastEntry gets the last entry in the map
	LastEntry(ctx context.Context) (*Entry, error)

	// PrevEntry gets the entry before the given index
	PrevEntry(ctx context.Context, index Index) (*Entry, error)

	// NextEntry gets the entry after the given index
	NextEntry(ctx context.Context, index Index) (*Entry, error)

	// Remove removes a key from the map
	Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error)

	// RemoveIndex removes an index from the map
	RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry, error)

	// Len returns the number of entries in the map
	Len(ctx context.Context) (int, error)

	// Clear removes all entries from the map
	Clear(ctx context.Context) error

	// Entries lists the entries in the map
	// This is a non-blocking method. If the method returns without error, key/value paids will be pushed on to the
	// given channel and the channel will be closed once all entries have been read from the map.
	Entries(ctx context.Context, ch chan<- Entry) error

	// Watch watches the map for changes
	// This is a non-blocking method. If the method returns without error, map events will be pushed onto
	// the given channel in the order in which they occur.
	Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error
}

// Entry is an indexed key/value pair
type Entry struct {
	meta.ObjectMeta

	// Index is the unique, monotonically increasing, globally unique index of the entry. The index is static
	// for the lifetime of a key.
	Index Index

	// Version is the unique, monotonically increasing version number for the key/value pair. The version is
	// suitable for use in optimistic locking.
	Version Version

	// Key is the key of the pair
	Key string

	// Value is the value of the pair
	Value []byte
}

func (kv Entry) String() string {
	return fmt.Sprintf("key: %s\nvalue: %s\nversion: %d", kv.Key, string(kv.Value), kv.Version)
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

	// EventReplay indicats an entry was replayed
	EventReplay EventType = "replay"
)

// Event is a map change event
type Event struct {
	// Type indicates the change event type
	Type EventType

	// Entry is the event entry
	Entry Entry
}

// New creates a new IndexedMap primitive
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (IndexedMap, error) {
	m := &indexedMap{
		Client: primitive.NewClient(Type, name, conn),
		client: api.NewIndexedMapServiceClient(conn),
	}
	if err := m.Create(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// indexedMap is the default single-partition implementation of Map
type indexedMap struct {
	*primitive.Client
	client api.IndexedMapServiceClient
}

func newEntry(entry *api.Entry) *Entry {
	if entry == nil {
		return nil
	}
	return &Entry{
		ObjectMeta: meta.FromProto(entry.Value.ObjectMeta),
		Index:      Index(entry.Index),
		Key:        entry.Key,
		Value:      entry.Value.Value,
	}
}

func (m *indexedMap) Append(ctx context.Context, key string, value []byte) (*Entry, error) {
	request := &api.PutRequest{
		Entry: api.Entry{
			Position: api.Position{
				Key: key,
			},
			Value: api.Value{
				Value: value,
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
	return newEntry(response.Entry), nil
}

func (m *indexedMap) Put(ctx context.Context, key string, value []byte) (*Entry, error) {
	request := &api.PutRequest{
		Entry: api.Entry{
			Position: api.Position{
				Key: key,
			},
			Value: api.Value{
				Value: value,
			},
		},
	}
	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newEntry(response.Entry), nil
}

func (m *indexedMap) Set(ctx context.Context, index Index, key string, value []byte, opts ...SetOption) (*Entry, error) {
	request := &api.PutRequest{
		Entry: api.Entry{
			Position: api.Position{
				Index: uint64(index),
				Key:   key,
			},
			Value: api.Value{
				Value: value,
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
	return newEntry(response.Entry), nil
}

func (m *indexedMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
	request := &api.GetRequest{
		Key: key,
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
	return newEntry(response.Entry), nil
}

func (m *indexedMap) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry, error) {
	request := &api.GetRequest{
		Index: uint64(index),
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
	return newEntry(response.Entry), nil
}

func (m *indexedMap) FirstIndex(ctx context.Context) (Index, error) {
	request := &api.FirstEntryRequest{}
	response, err := m.client.FirstEntry(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMap) LastIndex(ctx context.Context) (Index, error) {
	request := &api.LastEntryRequest{}
	response, err := m.client.LastEntry(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMap) PrevIndex(ctx context.Context, index Index) (Index, error) {
	request := &api.PrevEntryRequest{
		Index: uint64(index),
	}
	response, err := m.client.PrevEntry(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMap) NextIndex(ctx context.Context, index Index) (Index, error) {
	request := &api.NextEntryRequest{
		Index: uint64(index),
	}
	response, err := m.client.NextEntry(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMap) FirstEntry(ctx context.Context) (*Entry, error) {
	request := &api.FirstEntryRequest{}
	response, err := m.client.FirstEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newEntry(response.Entry), nil
}

func (m *indexedMap) LastEntry(ctx context.Context) (*Entry, error) {
	request := &api.LastEntryRequest{}
	response, err := m.client.LastEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newEntry(response.Entry), nil
}

func (m *indexedMap) PrevEntry(ctx context.Context, index Index) (*Entry, error) {
	request := &api.PrevEntryRequest{
		Index: uint64(index),
	}
	response, err := m.client.PrevEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newEntry(response.Entry), nil
}

func (m *indexedMap) NextEntry(ctx context.Context, index Index) (*Entry, error) {
	request := &api.NextEntryRequest{
		Index: uint64(index),
	}
	response, err := m.client.NextEntry(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newEntry(response.Entry), nil
}

func (m *indexedMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error) {
	request := &api.RemoveRequest{
		Entry: &api.Entry{
			Position: api.Position{
				Key: key,
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
	return newEntry(response.Entry), nil
}

func (m *indexedMap) RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry, error) {
	request := &api.RemoveRequest{
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
	return newEntry(response.Entry), nil
}

func (m *indexedMap) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{}
	response, err := m.client.Size(m.AddHeaders(ctx), request)
	if err != nil {
		return 0, errors.From(err)
	}
	return int(response.Size_), nil
}

func (m *indexedMap) Clear(ctx context.Context) error {
	request := &api.ClearRequest{}
	_, err := m.client.Clear(m.AddHeaders(ctx), request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (m *indexedMap) Entries(ctx context.Context, ch chan<- Entry) error {
	request := &api.EntriesRequest{}
	stream, err := m.client.Entries(m.AddHeaders(ctx), request)
	if err != nil {
		return errors.From(err)
	}

	openCh := make(chan struct{})
	go func() {
		defer close(ch)
		open := false
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				log.Errorf("Entries failed: %v", err)
			} else {
				if !open {
					close(openCh)
					open = true
				}
				ch <- *newEntry(&response.Entry)
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

func (m *indexedMap) Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error {
	request := &api.EventsRequest{}
	stream, err := m.client.Events(m.AddHeaders(ctx), request)
	if err != nil {
		return errors.From(err)
	}

	openCh := make(chan struct{})
	go func() {
		defer close(ch)
		open := false
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				log.Errorf("Watch failed: %v", err)
			} else {
				if !open {
					close(openCh)
					open = true
				}
				switch response.Event.Type {
				case api.Event_INSERT:
					ch <- Event{
						Type:  EventInsert,
						Entry: *newEntry(&response.Event.Entry),
					}
				case api.Event_UPDATE:
					ch <- Event{
						Type:  EventUpdate,
						Entry: *newEntry(&response.Event.Entry),
					}
				case api.Event_REMOVE:
					ch <- Event{
						Type:  EventRemove,
						Entry: *newEntry(&response.Event.Entry),
					}
				case api.Event_REPLAY:
					ch <- Event{
						Type:  EventReplay,
						Entry: *newEntry(&response.Event.Entry),
					}
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
