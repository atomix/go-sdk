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

package _map

import (
	"context"
	"fmt"
	mapapi "github.com/atomix/api/proto/atomix/gossip/map"
	"github.com/atomix/go-client/pkg/client/cluster"
	"github.com/atomix/go-client/pkg/client/primitive"
	"google.golang.org/grpc"
)

func init() {
	cluster.RegisterService(func(memberID cluster.MemberID, server *grpc.Server) {
		mapapi.RegisterGossipMapServiceServer(server, getManager(memberID))
	})
}

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

// Entry is a versioned key/value pair
type Entry struct {
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
	// EventNone indicates the event is not a change event
	EventNone EventType = ""

	// EventInserted indicates a key was newly created in the map
	EventInserted EventType = "inserted"

	// EventUpdated indicates the value of an existing key was changed
	EventUpdated EventType = "updated"

	// EventRemoved indicates a key was removed from the map
	EventRemoved EventType = "removed"
)

// Event is a map change event
type Event struct {
	// Type indicates the change event type
	Type EventType

	// Entry is the event entry
	Entry *Entry
}
