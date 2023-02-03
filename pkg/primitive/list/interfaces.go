// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"context"
	listv1 "github.com/atomix/atomix/api/runtime/list/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
)

var log = logging.GetLogger()

type Builder[E any] interface {
	primitive.Builder[Builder[E], List[E]]
	Codec(codec types.Codec[E]) Builder[E]
}

// List provides a distributed list data structure
// The list values are defines as strings. To store more complex types in the list, encode values to strings e.g.
// using base 64 encoding.
type List[E any] interface {
	primitive.Primitive

	// Append pushes a value on to the end of the list
	Append(ctx context.Context, value E) error

	// Insert inserts a value at the given index
	Insert(ctx context.Context, index int, value E) error

	// Set sets the value at the given index
	Set(ctx context.Context, index int, value E) error

	// Get gets the value at the given index
	Get(ctx context.Context, index int) (E, error)

	// Remove removes and returns the value at the given index
	Remove(ctx context.Context, index int) (E, error)

	// Len gets the length of the list
	Len(ctx context.Context) (int, error)

	// Clear removes all values from the list
	Clear(ctx context.Context) error

	// Items lists the elements in the set
	// This is a non-blocking method. If the method returns without error, key/value paids will be pushed on to the
	// given channel and the channel will be closed once all elements have been read from the set.
	Items(ctx context.Context) (ItemStream[E], error)

	// Watch watches the set for changes
	// This is a non-blocking method. If the method returns without error, set events will be pushed onto
	// the given channel in the order in which they occur.
	Watch(ctx context.Context) (ItemStream[E], error)

	// Events watches the set for change events
	// This is a non-blocking method. If the method returns without error, set events will be pushed onto
	// the given channel in the order in which they occur.
	Events(ctx context.Context, opts ...EventsOption) (EventStream[E], error)
}

type ItemStream[E any] stream.Stream[E]

type EventStream[E any] stream.Stream[Event[E]]

// Event is a map change event
type Event[E any] interface {
	event() *listv1.Event
}

type grpcEvent struct {
	proto *listv1.Event
}

func (e *grpcEvent) event() *listv1.Event {
	return e.proto
}

type Inserted[E any] struct {
	*grpcEvent
	Value E
}

type Updated[E any] struct {
	*grpcEvent
	Value     E
	PrevValue E
}

type Removed[E any] struct {
	*grpcEvent
	Value E
}
