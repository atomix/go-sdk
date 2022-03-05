// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	api "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive/codec"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger("atomix", "client", "value")

// Type is the value type
const Type primitive.Type = "Value"

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

// New creates a new Lock primitive for the given partitions
// The value will be created in one of the given partitions.
func New[V any](ctx context.Context, name string, conn *grpc.ClientConn, opts ...primitive.Option) (Value[V], error) {
	options := newValueOptions[V]{}
	for _, opt := range opts {
		if op, ok := opt.(Option[V]); ok {
			op.applyNewValue(&options)
		}
	}
	if options.valueCodec == nil {
		options.valueCodec = codec.String().(codec.Codec[V])
	}
	v := &typedValue[V]{
		Client: primitive.NewClient(Type, name, conn, opts...),
		client: api.NewValueServiceClient(conn),
		codec:  options.valueCodec,
	}
	if err := v.Create(ctx); err != nil {
		return nil, err
	}
	return v, nil
}

// value is the single partition implementation of Lock
type typedValue[V any] struct {
	*primitive.Client
	client api.ValueServiceClient
	codec  codec.Codec[V]
}

func (v *typedValue[V]) Set(ctx context.Context, value V, opts ...SetOption) (meta.ObjectMeta, error) {
	bytes, err := v.codec.Encode(value)
	if err != nil {
		return meta.ObjectMeta{}, errors.NewInvalid("value encoding failed", err)
	}
	request := &api.SetRequest{
		Headers: v.GetHeaders(),
		Value: api.Value{
			Value: bytes,
		},
	}
	for i := range opts {
		opts[i].beforeSet(request)
	}
	response, err := v.client.Set(ctx, request)
	if err != nil {
		return meta.ObjectMeta{}, errors.From(err)
	}
	for i := range opts {
		opts[i].afterSet(response)
	}
	return meta.FromProto(response.Value.ObjectMeta), nil
}

func (v *typedValue[V]) Get(ctx context.Context) (V, meta.ObjectMeta, error) {
	request := &api.GetRequest{
		Headers: v.GetHeaders(),
	}
	var r V
	response, err := v.client.Get(ctx, request)
	if err != nil {
		return r, meta.ObjectMeta{}, errors.From(err)
	}
	value, err := v.codec.Decode(response.Value.Value)
	if err != nil {
		return r, meta.ObjectMeta{}, errors.NewInvalid("value decoding failed", err)
	}
	return value, meta.FromProto(response.Value.ObjectMeta), nil
}

func (v *typedValue[V]) Watch(ctx context.Context, ch chan<- Event[V]) error {
	request := &api.EventsRequest{
		Headers: v.GetHeaders(),
	}
	stream, err := v.client.Events(ctx, request)
	if err != nil {
		return errors.From(err)
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
				err = errors.From(err)
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

			value, err := v.codec.Decode(response.Event.Value.Value)
			if err != nil {
				log.Error(err)
				continue
			}

			switch response.Event.Type {
			case api.Event_UPDATE:
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
