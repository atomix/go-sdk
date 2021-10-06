// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package value

import (
	"context"
	api "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger("atomix", "client", "value")

// Type is the value type
const Type primitive.Type = "Value"

// Client provides an API for creating Values
type Client interface {
	// GetValue gets the Value instance of the given name
	GetValue(ctx context.Context, name string, opts ...primitive.Option) (Value, error)
}

// Value provides a simple atomic value
type Value interface {
	primitive.Primitive

	// Set sets the current value and returns the version
	Set(ctx context.Context, value []byte, opts ...SetOption) (meta.ObjectMeta, error)

	// Get gets the current value and version
	Get(ctx context.Context) ([]byte, meta.ObjectMeta, error)

	// Watch watches the value for changes
	Watch(ctx context.Context, ch chan<- Event) error
}

// EventType is the type of a set event
type EventType string

const (
	// EventUpdate indicates the value was updated
	EventUpdate EventType = "update"
)

// Event is a value change event
type Event struct {
	meta.ObjectMeta

	// Type is the change event type
	Type EventType

	// Value is the updated value
	Value []byte
}

// New creates a new Lock primitive for the given partitions
// The value will be created in one of the given partitions.
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...primitive.Option) (Value, error) {
	options := newValueOptions{}
	for _, opt := range opts {
		if op, ok := opt.(Option); ok {
			op.applyNewValue(&options)
		}
	}
	v := &value{
		Client:  primitive.NewClient(Type, name, conn, opts...),
		client:  api.NewValueServiceClient(conn),
		options: options,
	}
	if err := v.Create(ctx); err != nil {
		return nil, err
	}
	return v, nil
}

// value is the single partition implementation of Lock
type value struct {
	*primitive.Client
	client  api.ValueServiceClient
	options newValueOptions
}

func (v *value) Set(ctx context.Context, value []byte, opts ...SetOption) (meta.ObjectMeta, error) {
	request := &api.SetRequest{
		Headers: v.GetHeaders(),
		Value: api.Value{
			Value: value,
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

func (v *value) Get(ctx context.Context) ([]byte, meta.ObjectMeta, error) {
	request := &api.GetRequest{
		Headers: v.GetHeaders(),
	}
	response, err := v.client.Get(ctx, request)
	if err != nil {
		return nil, meta.ObjectMeta{}, errors.From(err)
	}
	return response.Value.Value, meta.FromProto(response.Value.ObjectMeta), nil
}

func (v *value) Watch(ctx context.Context, ch chan<- Event) error {
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
			switch response.Event.Type {
			case api.Event_UPDATE:
				ch <- Event{
					ObjectMeta: meta.FromProto(response.Event.Value.ObjectMeta),
					Type:       EventUpdate,
					Value:      response.Event.Value.Value,
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
