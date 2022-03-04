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
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"google.golang.org/grpc"
)

const Type primitive.Type[Map] = "Map"

var log = logging.GetLogger("atomix", "client", "map")

// Client provides an API for creating Maps
type Client interface {
	// GetMap gets the Map instance of the given name
	GetMap(ctx context.Context, name string, opts ...primitive.Option[Map]) (Map, error)
}

// Map is a distributed set of keys and values
type Map[K, V any] interface {
	primitive.Primitive[Map[K, V]]

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
	return fmt.Sprintf("key: %s\nvalue: %s", kv.Key, kv.Value)
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
func New[K, V any](ctx context.Context, name string, conn *grpc.ClientConn, opts ...primitive.Option[Map]) (Map[K, V], error) {
	transcodingOptions := newMapOptions[K, V]{}
	for _, opt := range opts {
		if op, ok := opt.(Option[K, V]); ok {
			op.applyNewMap(&transcodingOptions)
		}
	}
	base := &baseMap{
		Client: primitive.NewClient[Map](Type, name, conn, opts...),
		client: api.NewMapServiceClient(conn),
	}
	if err := base.Create(ctx); err != nil {
		return nil, err
	}
	return newTranscodingMap[K, V, string, []byte](base, transcodingOptions.keyCodec, transcodingOptions.valueCodec), nil
}
