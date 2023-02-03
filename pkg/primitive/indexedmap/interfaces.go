// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"fmt"
	indexedmapv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
	"github.com/atomix/go-sdk/pkg/types/scalar"
)

var log = logging.GetLogger()

type Builder[K scalar.Scalar, V any] interface {
	primitive.Builder[Builder[K, V], IndexedMap[K, V]]
	Codec(codec types.Codec[V]) Builder[K, V]
}

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
	Entry     *Entry[K, V]
	PrevEntry *Entry[K, V]
}

type Removed[K scalar.Scalar, V any] struct {
	*grpcEvent
	Entry   *Entry[K, V]
	Expired bool
}
