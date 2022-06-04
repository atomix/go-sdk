// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/time"
	"io"
)

var log = logging.GetLogger()

// Value provides a simple atomic value
type Value[T any] interface {
	primitive.Primitive

	// Set sets the current value and returns the version
	Set(ctx context.Context, value T, opts ...SetOption) (time.Timestamp, error)

	// Get gets the current value and version
	Get(ctx context.Context) (T, time.Timestamp, error)

	// Watch watches the value for changes
	Watch(ctx context.Context, ch chan<- Event[T]) error
}

// EventType is the type of a value event
type EventType string

const (
	// EventUpdate indicates the value was updated
	EventUpdate EventType = "update"
)

// Event is a value change event
type Event[T any] struct {
	// Type is the change event type
	Type EventType

	// Value is the updated value
	Value T

	// Timestamp is the timestamp at which the event occurred
	Timestamp time.Timestamp
}

func New[T any](client valuev1.ValueClient) func(context.Context, primitive.ID, ...Option[T]) (Value[T], error) {
	return func(ctx context.Context, id primitive.ID, opts ...Option[T]) (Value[T], error) {
		var options Options[T]
		options.apply(opts...)
		if options.ValueType == nil {
			stringType := generic.Bytes()
			if valueType, ok := stringType.(generic.Type[T]); ok {
				options.ValueType = valueType
			} else {
				return nil, errors.NewInvalid("must configure a generic type for value parameter")
			}
		}
		indexedMap := &valuePrimitive[T]{
			Primitive: primitive.New(id),
			client:    client,
			valueType: options.ValueType,
		}
		if err := indexedMap.create(ctx); err != nil {
			return nil, err
		}
		return indexedMap, nil
	}
}

// value is the single partition implementation of Lock
type valuePrimitive[T any] struct {
	primitive.Primitive
	client    valuev1.ValueClient
	valueType generic.Type[T]
}

func (v *valuePrimitive[T]) Set(ctx context.Context, value T, opts ...SetOption) (time.Timestamp, error) {
	bytes, err := v.valueType.Marshal(&value)
	if err != nil {
		return nil, errors.NewInvalid("element encoding failed", err)
	}
	request := &valuev1.SetRequest{
		Value: bytes,
	}
	for i := range opts {
		opts[i].beforeSet(request)
	}
	response, err := v.client.Set(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterSet(response)
	}
	return time.NewTimestamp(*response.Timestamp), nil
}

func (v *valuePrimitive[T]) Get(ctx context.Context) (T, time.Timestamp, error) {
	request := &valuev1.GetRequest{}
	var r T
	response, err := v.client.Get(ctx, request)
	if err != nil {
		return r, nil, errors.FromProto(err)
	}
	var value T
	if err := v.valueType.Unmarshal(response.Value, &value); err != nil {
		return value, nil, err
	}
	return value, time.NewTimestamp(*response.Timestamp), nil
}

func (v *valuePrimitive[T]) Watch(ctx context.Context, ch chan<- Event[T]) error {
	request := &valuev1.EventsRequest{}
	stream, err := v.client.Events(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}

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
			response, err := stream.Recv()
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

			var value T
			if err := v.valueType.Unmarshal(response.Event.Value, &value); err != nil {
				log.Error(err)
				continue
			}

			switch response.Event.Type {
			case valuev1.Event_UPDATE:
				ch <- Event[T]{
					Type:      EventUpdate,
					Value:     value,
					Timestamp: time.NewTimestamp(*response.Event.Timestamp),
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

func (v *valuePrimitive[T]) create(ctx context.Context) error {
	request := &valuev1.CreateRequest{
		Config: valuev1.ValueConfig{},
	}
	ctx = primitive.AppendToOutgoingContext(ctx, v.ID())
	_, err := v.client.Create(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (v *valuePrimitive[T]) Close(ctx context.Context) error {
	request := &valuev1.CloseRequest{}
	ctx = primitive.AppendToOutgoingContext(ctx, v.ID())
	_, err := v.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
