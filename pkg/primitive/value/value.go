// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/go-sdk/pkg/generic"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	valuev1 "github.com/atomix/runtime/api/atomix/runtime/value/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"io"
)

var log = logging.GetLogger()

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
	OldValue primitive.Versioned[V]
	NewValue primitive.Versioned[V]
}

type Deleted[V any] struct {
	*grpcEvent
	Value   primitive.Versioned[V]
	Expired bool
}

type atomicValuePrimitive[V any] struct {
	primitive.Primitive
	client valuev1.ValueClient
	codec  generic.Codec[V]
}

func (m *atomicValuePrimitive[V]) Set(ctx context.Context, value V, opts ...SetOption) (primitive.Versioned[V], error) {
	bytes, err := m.codec.Encode(value)
	if err != nil {
		return primitive.Versioned[V]{}, errors.NewInvalid("value encoding failed", err)
	}
	request := &valuev1.SetRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Value: bytes,
	}
	for i := range opts {
		opts[i].beforeSet(request)
	}
	response, err := m.client.Set(ctx, request)
	if err != nil {
		return primitive.Versioned[V]{}, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterSet(response)
	}
	return primitive.Versioned[V]{
		Version: primitive.Version(response.Version),
		Value:   value,
	}, nil
}

func (m *atomicValuePrimitive[V]) Update(ctx context.Context, value V, opts ...UpdateOption) (primitive.Versioned[V], error) {
	bytes, err := m.codec.Encode(value)
	if err != nil {
		return primitive.Versioned[V]{}, errors.NewInvalid("value encoding failed", err)
	}
	request := &valuev1.UpdateRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Value: bytes,
	}
	for i := range opts {
		opts[i].beforeUpdate(request)
	}
	response, err := m.client.Update(ctx, request)
	if err != nil {
		return primitive.Versioned[V]{}, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterUpdate(response)
	}
	return primitive.Versioned[V]{
		Version: primitive.Version(response.Version),
		Value:   value,
	}, nil
}

func (m *atomicValuePrimitive[V]) Get(ctx context.Context, opts ...GetOption) (primitive.Versioned[V], error) {
	request := &valuev1.GetRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := m.client.Get(ctx, request)
	if err != nil {
		return primitive.Versioned[V]{}, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	value, err := m.codec.Decode(response.Value.Value)
	if err != nil {
		return primitive.Versioned[V]{}, errors.NewInvalid("value decoding failed", err)
	}
	return primitive.Versioned[V]{
		Version: primitive.Version(response.Value.Version),
		Value:   value,
	}, nil
}

func (m *atomicValuePrimitive[V]) Delete(ctx context.Context, opts ...DeleteOption) error {
	request := &valuev1.DeleteRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeDelete(request)
	}
	response, err := m.client.Delete(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterDelete(response)
	}
	return nil
}

func (m *atomicValuePrimitive[V]) Watch(ctx context.Context) (ValueStream[V], error) {
	request := &valuev1.WatchRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	client, err := m.client.Watch(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}

	ch := make(chan stream.Result[primitive.Versioned[V]])
	go func() {
		defer close(ch)
		for {
			response, err := client.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				err = errors.FromProto(err)
				if errors.IsCanceled(err) || errors.IsTimeout(err) {
					return
				}
				log.Errorf("Watch failed: %v", err)
				return
			}
			value, err := m.codec.Decode(response.Value.Value)
			if err != nil {
				log.Error(err)
			} else {
				ch <- stream.Result[primitive.Versioned[V]]{
					Value: primitive.Versioned[V]{
						Version: primitive.Version(response.Value.Version),
						Value:   value,
					},
				}
			}
		}
	}()
	return stream.NewChannelStream[primitive.Versioned[V]](ch), nil
}

func (m *atomicValuePrimitive[V]) Events(ctx context.Context, opts ...EventsOption) (EventStream[V], error) {
	request := &valuev1.EventsRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeEvents(request)
	}

	client, err := m.client.Events(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}

	ch := make(chan stream.Result[Event[V]])
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
				err = errors.FromProto(err)
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
			case *valuev1.Event_Created_:
				value, err := m.codec.Decode(e.Created.Value.Value)
				if err != nil {
					log.Error(err)
					continue
				}

				ch <- stream.Result[Event[V]]{
					Value: &Created[V]{
						grpcEvent: &grpcEvent{&response.Event},
						Value: primitive.Versioned[V]{
							Version: primitive.Version(e.Created.Value.Version),
							Value:   value,
						},
					},
				}
			case *valuev1.Event_Updated_:
				newValue, err := m.codec.Decode(e.Updated.Value.Value)
				if err != nil {
					log.Error(err)
					continue
				}
				oldValue, err := m.codec.Decode(e.Updated.PrevValue.Value)
				if err != nil {
					log.Error(err)
					continue
				}

				ch <- stream.Result[Event[V]]{
					Value: &Updated[V]{
						grpcEvent: &grpcEvent{&response.Event},
						NewValue: primitive.Versioned[V]{
							Version: primitive.Version(e.Updated.Value.Version),
							Value:   newValue,
						},
						OldValue: primitive.Versioned[V]{
							Version: primitive.Version(e.Updated.PrevValue.Version),
							Value:   oldValue,
						},
					},
				}
			case *valuev1.Event_Deleted_:
				value, err := m.codec.Decode(e.Deleted.Value.Value)
				if err != nil {
					log.Error(err)
					continue
				}

				ch <- stream.Result[Event[V]]{
					Value: &Deleted[V]{
						grpcEvent: &grpcEvent{&response.Event},
						Value: primitive.Versioned[V]{
							Version: primitive.Version(e.Deleted.Value.Version),
							Value:   value,
						},
						Expired: e.Deleted.Expired,
					},
				}
			}
		}
	}()

	select {
	case <-openCh:
		return stream.NewChannelStream[Event[V]](ch), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *atomicValuePrimitive[V]) create(ctx context.Context, tags ...string) error {
	request := &valuev1.CreateRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Tags: tags,
	}
	_, err := m.client.Create(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (m *atomicValuePrimitive[V]) Close(ctx context.Context) error {
	request := &valuev1.CloseRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	_, err := m.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
