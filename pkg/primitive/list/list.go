// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	listv1 "github.com/atomix/atomix/api/runtime/list/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
	"io"
)

var log = logging.GetLogger()

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
	NewValue E
	OldValue E
}

type Removed[E any] struct {
	*grpcEvent
	Value E
}

// listPrimitive is the single partition implementation of List
type listPrimitive[E any] struct {
	primitive.Primitive
	client listv1.ListClient
	codec  types.Codec[E]
}

func (l *listPrimitive[E]) Append(ctx context.Context, value E) error {
	bytes, err := l.codec.Encode(value)
	if err != nil {
		return errors.NewInvalid("value encoding failed", err)
	}
	request := &listv1.AppendRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Value: listv1.Value{
			Value: bytes,
		},
	}
	_, err = l.client.Append(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (l *listPrimitive[E]) Insert(ctx context.Context, index int, value E) error {
	bytes, err := l.codec.Encode(value)
	if err != nil {
		return errors.NewInvalid("value encoding failed", err)
	}
	request := &listv1.InsertRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Index: uint32(index),
		Value: listv1.Value{
			Value: bytes,
		},
	}
	_, err = l.client.Insert(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (l *listPrimitive[E]) Set(ctx context.Context, index int, value E) error {
	bytes, err := l.codec.Encode(value)
	if err != nil {
		return errors.NewInvalid("value encoding failed", err)
	}
	request := &listv1.SetRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Index: uint32(index),
		Value: listv1.Value{
			Value: bytes,
		},
	}
	_, err = l.client.Set(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (l *listPrimitive[E]) Get(ctx context.Context, index int) (E, error) {
	request := &listv1.GetRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Index: uint32(index),
	}
	var item E
	response, err := l.client.Get(ctx, request)
	if err != nil {
		return item, err
	}
	return l.codec.Decode(response.Item.Value.Value)
}

func (l *listPrimitive[E]) Remove(ctx context.Context, index int) (E, error) {
	request := &listv1.RemoveRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Index: uint32(index),
	}
	var e E
	response, err := l.client.Remove(ctx, request)
	if err != nil {
		return e, err
	}
	return l.codec.Decode(response.Item.Value.Value)
}

func (l *listPrimitive[E]) Len(ctx context.Context) (int, error) {
	request := &listv1.SizeRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
	}
	response, err := l.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}
	return int(response.Size_), nil
}

func (l *listPrimitive[E]) Items(ctx context.Context) (ItemStream[E], error) {
	return l.items(ctx, false)
}

func (l *listPrimitive[E]) Watch(ctx context.Context) (ItemStream[E], error) {
	return l.items(ctx, true)
}

func (l *listPrimitive[E]) items(ctx context.Context, watch bool) (ItemStream[E], error) {
	request := &listv1.ItemsRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Watch: watch,
	}
	client, err := l.client.Items(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[E])
	go func() {
		defer close(ch)
		for {
			response, err := client.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				if errors.IsCanceled(err) || errors.IsTimeout(err) {
					return
				}
				log.Errorf("Entries failed: %v", err)
				return
			}
			item, err := l.codec.Decode(response.Item.Value.Value)
			if err != nil {
				log.Error(err)
				continue
			}
			ch <- stream.Result[E]{
				Value: item,
			}
		}
	}()
	return stream.NewChannelStream[E](ch), nil
}

func (l *listPrimitive[E]) Events(ctx context.Context, opts ...EventsOption) (EventStream[E], error) {
	request := &listv1.EventsRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeEvents(request)
	}

	client, err := l.client.Events(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[Event[E]])
	openCh := make(chan struct{})
	go func() {
		defer close(ch)
		open := false
		defer func() {
			if !open {
				close(openCh)
			}
		}()
		for {
			response, err := client.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				if errors.IsCanceled(err) || errors.IsTimeout(err) {
					return
				}
				log.Errorf("Watch failed: %v", err)
				return
			}

			if !open {
				close(openCh)
				open = true
			}

			for i := range opts {
				opts[i].afterEvents(response)
			}

			switch e := response.Event.Event.(type) {
			case *listv1.Event_Inserted_:
				value, err := l.codec.Decode(e.Inserted.Value.Value)
				if err != nil {
					log.Error(err)
					continue
				}
				ch <- stream.Result[Event[E]]{
					Value: &Inserted[E]{
						grpcEvent: &grpcEvent{&response.Event},
						Value:     value,
					},
				}
			case *listv1.Event_Updated_:
				oldValue, err := l.codec.Decode(e.Updated.Value.Value)
				if err != nil {
					log.Error(err)
					continue
				}
				newValue, err := l.codec.Decode(e.Updated.PrevValue.Value)
				if err != nil {
					log.Error(err)
					continue
				}

				ch <- stream.Result[Event[E]]{
					Value: &Updated[E]{
						grpcEvent: &grpcEvent{&response.Event},
						NewValue:  newValue,
						OldValue:  oldValue,
					},
				}
			case *listv1.Event_Removed_:
				value, err := l.codec.Decode(e.Removed.Value.Value)
				if err != nil {
					log.Error(err)
					continue
				}
				ch <- stream.Result[Event[E]]{
					Value: &Removed[E]{
						grpcEvent: &grpcEvent{&response.Event},
						Value:     value,
					},
				}
			}
		}
	}()

	select {
	case <-openCh:
		return stream.NewChannelStream[Event[E]](ch), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *listPrimitive[E]) Clear(ctx context.Context) error {
	request := &listv1.ClearRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
	}
	_, err := l.client.Clear(ctx, request)
	if err != nil {
		return err
	}
	return nil
}
