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
	api "github.com/atomix/atomix-api/go/atomix/primitive/map"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"google.golang.org/grpc"
	"io"
)

// Type is the map type
const Type primitive.Type = "Map"

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
		ObjectMeta: meta.FromProto(entry.Key.ObjectMeta),
		Key:        entry.Key.Key,
		Value:      entry.Value.Value,
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
	m := &_map{
		Client: primitive.NewClient(Type, name, conn),
		client: api.NewMapServiceClient(conn),
	}
	if err := m.Create(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

type _map struct {
	*primitive.Client
	client api.MapServiceClient
}

func (m *_map) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry, error) {
	request := &api.PutRequest{
		Headers: m.GetHeaders(),
		Entry: api.Entry{
			Key: api.Key{
				Key: key,
			},
			Value: &api.Value{
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
	return newEntry(&response.Entry), nil
}

func (m *_map) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
	request := &api.GetRequest{
		Headers: m.GetHeaders(),
		Key:     key,
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
	return newEntry(&response.Entry), nil
}

func (m *_map) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error) {
	request := &api.RemoveRequest{
		Headers: m.GetHeaders(),
		Key: api.Key{
			Key: key,
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
	return newEntry(&response.Entry), nil
}

func (m *_map) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Headers: m.GetHeaders(),
	}
	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return int(response.Size_), nil
}

func (m *_map) Clear(ctx context.Context) error {
	request := &api.ClearRequest{
		Headers: m.GetHeaders(),
	}
	_, err := m.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (m *_map) Entries(ctx context.Context, ch chan<- Entry) error {
	request := &api.EntriesRequest{
		Headers: m.GetHeaders(),
	}
	stream, err := m.client.Entries(ctx, request)
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
				ch <- Entry{
					ObjectMeta: meta.FromProto(response.Entry.Key.ObjectMeta),
					Key:        response.Entry.Key.Key,
					Value:      response.Entry.Value.Value,
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

func (m *_map) Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error {
	request := &api.EventsRequest{
		Headers: m.GetHeaders(),
	}
	stream, err := m.client.Events(ctx, request)
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
