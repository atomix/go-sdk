// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
)

var log = logging.GetLogger()

type Builder[E any] interface {
	primitive.Builder[Builder[E], Set[E]]
	Codec(codec types.Codec[E]) Builder[E]
}

// Set provides a distributed set data structure
// The set values are defines as strings. To store more complex types in the set, encode values to strings e.g.
// using base 64 encoding.
type Set[E any] interface {
	primitive.Primitive

	// Add adds a value to the set
	Add(ctx context.Context, value E) (bool, error)

	// Remove removes a value from the set
	// A bool indicating whether the set contained the given value will be returned
	Remove(ctx context.Context, value E) (bool, error)

	// Contains returns a bool indicating whether the set contains the given value
	Contains(ctx context.Context, value E) (bool, error)

	// Len gets the set size in number of elements
	Len(ctx context.Context) (int, error)

	// Clear removes all values from the set
	Clear(ctx context.Context) error

	// Elements lists the elements in the set
	// This is a non-blocking method. If the method returns without error, key/value paids will be pushed on to the
	// given channel and the channel will be closed once all elements have been read from the set.
	Elements(ctx context.Context) (ElementStream[E], error)

	// Watch watches the set for changes
	// This is a non-blocking method. If the method returns without error, set events will be pushed onto
	// the given channel in the order in which they occur.
	Watch(ctx context.Context) (ElementStream[E], error)

	// Events watches the set for change events
	// This is a non-blocking method. If the method returns without error, set events will be pushed onto
	// the given channel in the order in which they occur.
	Events(ctx context.Context) (EventStream[E], error)
}

type ElementStream[E any] stream.Stream[E]

type EventStream[E any] stream.Stream[Event[E]]

// Event is a set change event
type Event[E any] interface {
	event() *setv1.Event
}

type grpcEvent struct {
	proto *setv1.Event
}

func (e *grpcEvent) event() *setv1.Event {
	return e.proto
}

type Added[E any] struct {
	*grpcEvent
	Element E
}

type Removed[E any] struct {
	*grpcEvent
	Element E
	Expired bool
}
