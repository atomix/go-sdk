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
	"github.com/atomix/go-client/pkg/client/meta"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-framework/pkg/atomix/client"
	indexedmapclient "github.com/atomix/go-framework/pkg/atomix/client/indexedmap"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"google.golang.org/grpc"
)

// Type is the counter type
const Type = primitive.Type(indexedmapclient.PrimitiveType)

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
	options := applyOptions(opts...)
	m := &indexedMap{
		client: indexedmapclient.NewClient(client.ID(options.clientID), name, conn),
	}
	if err := m.create(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// indexedMap is the default single-partition implementation of Map
type indexedMap struct {
	client indexedmapclient.Client
}

func (m *indexedMap) Type() primitive.Type {
	return Type
}

func (m *indexedMap) Name() string {
	return m.client.Name()
}

func newEntry(entry *api.Entry) *Entry {
	if entry == nil {
		return nil
	}
	return &Entry{
		ObjectMeta: meta.New(entry.Value.Meta),
		Index:      Index(entry.Pos.Index),
		Key:        entry.Pos.Key,
		Value:      entry.Value.Value,
	}
}

func (m *indexedMap) Append(ctx context.Context, key string, value []byte) (*Entry, error) {
	input := &api.PutInput{
		Entry: api.Entry{
			Pos: api.Position{
				Key: key,
			},
			Value: api.Value{
				Value: value,
			},
		},
		IfEmpty: true,
	}
	output, err := m.client.Put(ctx, input)
	if err != nil {
		return nil, err
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) Put(ctx context.Context, key string, value []byte) (*Entry, error) {
	input := &api.PutInput{
		Entry: api.Entry{
			Pos: api.Position{
				Key: key,
			},
			Value: api.Value{
				Value: value,
			},
		},
	}
	output, err := m.client.Put(ctx, input)
	if err != nil {
		return nil, err
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) Set(ctx context.Context, index Index, key string, value []byte, opts ...SetOption) (*Entry, error) {
	input := &api.PutInput{
		Entry: api.Entry{
			Pos: api.Position{
				Index: uint64(index),
				Key:   key,
			},
			Value: api.Value{
				Value: value,
			},
		},
	}
	for i := range opts {
		opts[i].beforePut(input)
	}
	output, err := m.client.Put(ctx, input)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterPut(output)
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
	input := &api.GetInput{
		Key: key,
	}
	for i := range opts {
		opts[i].beforeGet(input)
	}
	output, err := m.client.Get(ctx, input)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterGet(output)
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry, error) {
	input := &api.GetInput{
		Index: uint64(index),
	}
	for i := range opts {
		opts[i].beforeGet(input)
	}
	output, err := m.client.Get(ctx, input)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterGet(output)
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) FirstIndex(ctx context.Context) (Index, error) {
	output, err := m.client.FirstEntry(ctx)
	if err != nil {
		return 0, err
	}
	return Index(output.Entry.Pos.Index), nil
}

func (m *indexedMap) LastIndex(ctx context.Context) (Index, error) {
	output, err := m.client.LastEntry(ctx)
	if err != nil {
		return 0, err
	}
	return Index(output.Entry.Pos.Index), nil
}

func (m *indexedMap) PrevIndex(ctx context.Context, index Index) (Index, error) {
	input := &api.PrevEntryInput{
		Index: uint64(index),
	}
	output, err := m.client.PrevEntry(ctx, input)
	if err != nil {
		return 0, err
	}
	return Index(output.Entry.Pos.Index), nil
}

func (m *indexedMap) NextIndex(ctx context.Context, index Index) (Index, error) {
	input := &api.NextEntryInput{
		Index: uint64(index),
	}
	output, err := m.client.NextEntry(ctx, input)
	if err != nil {
		return 0, err
	}
	return Index(output.Entry.Pos.Index), nil
}

func (m *indexedMap) FirstEntry(ctx context.Context) (*Entry, error) {
	output, err := m.client.FirstEntry(ctx)
	if err != nil {
		return nil, err
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) LastEntry(ctx context.Context) (*Entry, error) {
	output, err := m.client.LastEntry(ctx)
	if err != nil {
		return nil, err
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) PrevEntry(ctx context.Context, index Index) (*Entry, error) {
	input := &api.PrevEntryInput{
		Index: uint64(index),
	}
	output, err := m.client.PrevEntry(ctx, input)
	if err != nil {
		return nil, err
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) NextEntry(ctx context.Context, index Index) (*Entry, error) {
	input := &api.NextEntryInput{
		Index: uint64(index),
	}
	output, err := m.client.NextEntry(ctx, input)
	if err != nil {
		return nil, err
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error) {
	input := &api.RemoveInput{
		Entry: &api.Entry{
			Pos: api.Position{
				Key: key,
			},
		},
	}
	for i := range opts {
		opts[i].beforeRemove(input)
	}
	output, err := m.client.Remove(ctx, input)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterRemove(output)
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry, error) {
	input := &api.RemoveInput{
		Entry: &api.Entry{
			Pos: api.Position{
				Index: uint64(index),
			},
		},
	}
	for i := range opts {
		opts[i].beforeRemove(input)
	}
	output, err := m.client.Remove(ctx, input)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterRemove(output)
	}
	return newEntry(output.Entry), nil
}

func (m *indexedMap) Len(ctx context.Context) (int, error) {
	output, err := m.client.Size(ctx)
	if err != nil {
		return 0, err
	}
	return int(output.Size_), nil
}

func (m *indexedMap) Clear(ctx context.Context) error {
	return m.client.Clear(ctx)
}

func (m *indexedMap) Entries(ctx context.Context, ch chan<- Entry) error {
	input := &api.EntriesInput{}
	outputCh := make(chan api.EntriesOutput)
	if err := m.client.Entries(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
		defer close(ch)
		for output := range outputCh {
			ch <- *newEntry(&output.Entry)
		}
	}()
	return nil
}

func (m *indexedMap) Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error {
	input := &api.EventsInput{}
	for _, opt := range opts {
		opt.beforeWatch(input)
	}
	outputCh := make(chan api.EventsOutput)
	if err := m.client.Events(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
		defer close(ch)
		for output := range outputCh {
			var eventType EventType
			switch output.Type {
			case api.EventsOutput_INSERT:
				eventType = EventInsert
			case api.EventsOutput_UPDATE:
				eventType = EventUpdate
			case api.EventsOutput_REMOVE:
				eventType = EventRemove
			case api.EventsOutput_REPLAY:
				eventType = EventReplay
			}
			ch <- Event{
				Type: eventType,
				Entry: Entry{
					ObjectMeta: meta.New(output.Entry.Value.Meta),
					Key:        output.Entry.Pos.Key,
					Value:      output.Entry.Value.Value,
					Index:      Index(output.Entry.Pos.Index),
				},
			}
		}
	}()
	return nil
}

func (m *indexedMap) create(ctx context.Context) error {
	return m.client.Create(ctx)
}

func (m *indexedMap) Close(ctx context.Context) error {
	return m.client.Close(ctx)
}

func (m *indexedMap) Delete(ctx context.Context) error {
	return m.client.Delete(ctx)
}
