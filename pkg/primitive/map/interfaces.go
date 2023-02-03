// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	"fmt"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
	"github.com/atomix/go-sdk/pkg/types/scalar"
)

var log = logging.GetLogger()

type Builder[K scalar.Scalar, V any] interface {
	primitive.Builder[Builder[K, V], Map[K, V]]
	Codec(codec types.Codec[V]) Builder[K, V]
}

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
	Entry     *Entry[K, V]
	PrevEntry *Entry[K, V]
}

type Removed[K scalar.Scalar, V any] struct {
	*grpcEvent
	Entry   *Entry[K, V]
	Expired bool
}
