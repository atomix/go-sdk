// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	valuev1 "github.com/atomix/atomix/api/runtime/value/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
)

var log = logging.GetLogger()

type Builder[V any] interface {
	primitive.Builder[Builder[V], Value[V]]
	Codec(codec types.Codec[V]) Builder[V]
}

// Value is a distributed value supporting atomic check-and-set operations
type Value[V any] interface {
	primitive.Primitive

	// Set sets the value
	Set(ctx context.Context, value V, opts ...SetOption) (primitive.Versioned[V], error)

	// Update updates the value
	Update(ctx context.Context, value V, opts ...UpdateOption) (primitive.Versioned[V], error)

	// Get gets the value
	Get(ctx context.Context, opts ...GetOption) (primitive.Versioned[V], error)

	// Delete deletes the value
	Delete(ctx context.Context, opts ...DeleteOption) error

	// Watch watches the map for changes
	// This is a non-blocking method. If the method returns without error, map events will be pushed onto
	// the given channel in the order in which they occur.
	Watch(ctx context.Context) (ValueStream[V], error)

	// Events watches the map for change events
	// This is a non-blocking method. If the method returns without error, map events will be pushed onto
	// the given channel in the order in which they occur.
	Events(ctx context.Context, opts ...EventsOption) (EventStream[V], error)
}

type ValueStream[V any] stream.Stream[primitive.Versioned[V]]

type EventStream[V any] stream.Stream[Event[V]]

// Event is a map change event
type Event[V any] interface {
	event() *valuev1.Event
}

type grpcEvent struct {
	proto *valuev1.Event
}

func (e *grpcEvent) event() *valuev1.Event {
	return e.proto
}

type Created[V any] struct {
	*grpcEvent
	Value primitive.Versioned[V]
}

type Updated[V any] struct {
	*grpcEvent
	PrevValue primitive.Versioned[V]
	Value     primitive.Versioned[V]
}

type Deleted[V any] struct {
	*grpcEvent
	Value   primitive.Versioned[V]
	Expired bool
}
