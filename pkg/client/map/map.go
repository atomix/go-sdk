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

package _map //nolint:golint

import (
	"context"
	"fmt"
	api "github.com/atomix/api/go/atomix/primitive/map"
	"github.com/atomix/go-client/pkg/client/meta"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-framework/pkg/atomix/client"
	mapclient "github.com/atomix/go-framework/pkg/atomix/client/map"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"google.golang.org/grpc"
)

// Type is the counter type
const Type = primitive.Type(mapclient.PrimitiveType)

var log = logging.GetLogger("atomix", "client", "map")

// Client provides an API for creating Maps
type Client interface {
	// GetMap gets the Map instance of the given name
	GetMap(ctx context.Context, name string, opts ...Option) (Map, error)
}

// Map is a distributed set of keys and values
type Map interface {
	primitive.Primitive

	// Put sets a key/value pair in the map
	Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry, error)

	// Get gets the value of the given key
	Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error)

	// Remove removes a key from the map
	Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error)

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

// Version is an entry version
type Version uint64

func newEntry(entry *api.Entry) *Entry {
	if entry == nil {
		return nil
	}
	return &Entry{
		ObjectMeta: meta.New(entry.Meta),
		Key:        entry.Key,
		Value:      entry.Value,
	}
}

// Entry is a versioned key/value pair
type Entry struct {
	meta.ObjectMeta

	// Key is the key of the pair
	Key string

	// Value is the value of the pair
	Value []byte
}

func (kv Entry) String() string {
	return fmt.Sprintf("key: %s\nvalue: %s", kv.Key, string(kv.Value))
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
	EventReplay EventType = "remove"
)

// Event is a map change event
type Event struct {
	// Type indicates the change event type
	Type EventType

	// Entry is the event entry
	Entry Entry
}

// New creates a new partitioned Map
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (Map, error) {
	options := applyOptions(opts...)
	m := &_map{
		client: mapclient.NewClient(client.ID(options.clientID), name, conn),
	}
	if err := m.create(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

type _map struct {
	client mapclient.Client
}

func (m *_map) Type() primitive.Type {
	return Type
}

func (m *_map) Name() string {
	return m.client.Name()
}

func (m *_map) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry, error) {
	input := &api.PutInput{
		Key:   key,
		Value: value,
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

func (m *_map) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
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

func (m *_map) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error) {
	input := &api.RemoveInput{
		Key: key,
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

func (m *_map) Len(ctx context.Context) (int, error) {
	output, err := m.client.Size(ctx)
	if err != nil {
		return 0, err
	}
	return int(output.Size_), nil
}

func (m *_map) Clear(ctx context.Context) error {
	return m.client.Clear(ctx)
}

func (m *_map) Entries(ctx context.Context, ch chan<- Entry) error {
	input := &api.EntriesInput{}
	outputCh := make(chan api.EntriesOutput)
	if err := m.client.Entries(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
		for output := range outputCh {
			ch <- *newEntry(&output.Entry)
		}
	}()
	return nil
}

func (m *_map) Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error {
	input := &api.EventsInput{}
	for _, opt := range opts {
		opt.beforeWatch(input)
	}
	outputCh := make(chan api.EventsOutput)
	if err := m.client.Events(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
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
				Type:  eventType,
				Entry: *newEntry(&output.Entry),
			}
		}
	}()
	return nil
}

func (m *_map) create(ctx context.Context) error {
	return m.client.Create(ctx)
}

func (m *_map) Close(ctx context.Context) error {
	return m.client.Close(ctx)
}

func (m *_map) Delete(ctx context.Context) error {
	return m.client.Delete(ctx)
}
