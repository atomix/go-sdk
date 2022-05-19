// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/meta"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger()

// Value provides a simple atomic value
type Value[V any] interface {
	primitive.Primitive

	// Set sets the current value and returns the version
	Set(ctx context.Context, value V, opts ...SetOption) (meta.ObjectMeta, error)

	// Get gets the current value and version
	Get(ctx context.Context) (V, meta.ObjectMeta, error)

	// Watch watches the value for changes
	Watch(ctx context.Context, ch chan<- Event[V]) error
}

// EventType is the type of a set event
type EventType string

const (
	// EventUpdate indicates the value was updated
	EventUpdate EventType = "update"
)

// Event is a value change event
type Event[V any] struct {
	meta.ObjectMeta

	// Type is the change event type
	Type EventType

	// Value is the updated value
	Value V
}

func Client[V any](conn *grpc.ClientConn) primitive.Client[Value[V], Option[V]] {
	return primitive.NewClient[Value[V], Option[V]](newManager(conn), func(primitive *primitive.ManagedPrimitive, opts ...Option[V]) (Value[V], error) {
		var options Options[V]
		for _, opt := range opts {
			opt.apply(&options)
		}
		if options.ValueType == nil {
			stringType := generic.Bytes()
			if valueType, ok := stringType.(generic.Type[V]); ok {
				options.ValueType = valueType
			} else {
				return nil, errors.NewInvalid("must configure a generic type for key parameter")
			}
		}
		return &typedValue[V]{
			ManagedPrimitive: primitive,
			client:           valuev1.NewValueClient(conn),
			valueType:        options.ValueType,
		}, nil
	})
}

// value is the single partition implementation of Lock
type typedValue[V any] struct {
	*primitive.ManagedPrimitive
	client    valuev1.ValueClient
	valueType generic.Type[V]
}

func (v *typedValue[V]) Set(ctx context.Context, value V, opts ...SetOption) (meta.ObjectMeta, error) {
	bytes, err := v.valueType.Marshal(&value)
	if err != nil {
		return meta.ObjectMeta{}, errors.NewInvalid("element encoding failed", err)
	}
	request := &valuev1.SetRequest{
		Headers: v.GetHeaders(),
		SetInput: valuev1.SetInput{
			Value: valuev1.Object{
				Value: bytes,
			},
		},
	}
	for i := range opts {
		opts[i].beforeSet(request)
	}
	response, err := v.client.Set(ctx, request)
	if err != nil {
		return meta.ObjectMeta{}, errors.FromProto(err)
	}
	for i := range opts {
		opts[i].afterSet(response)
	}
	return meta.FromProto(response.Value.ObjectMeta), nil
}

func (v *typedValue[V]) Get(ctx context.Context) (V, meta.ObjectMeta, error) {
	request := &valuev1.GetRequest{
		Headers: v.GetHeaders(),
	}
	var r V
	response, err := v.client.Get(ctx, request)
	if err != nil {
		return r, meta.ObjectMeta{}, errors.FromProto(err)
	}
	var value V
	if err := v.valueType.Unmarshal(response.Value.Value, &value); err != nil {
		return value, meta.ObjectMeta{}, err
	}
	return value, meta.FromProto(response.Value.ObjectMeta), nil
}

func (v *typedValue[V]) Watch(ctx context.Context, ch chan<- Event[V]) error {
	request := &valuev1.EventsRequest{
		Headers: v.GetHeaders(),
	}
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

			var value V
			if err := v.valueType.Unmarshal(response.Event.Value.Value, &value); err != nil {
				log.Error(err)
				continue
			}

			switch response.Event.Type {
			case valuev1.Event_UPDATE:
				ch <- Event[V]{
					ObjectMeta: meta.FromProto(response.Event.Value.ObjectMeta),
					Type:       EventUpdate,
					Value:      value,
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
