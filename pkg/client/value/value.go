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
	api "github.com/atomix/api/go/atomix/primitive/value"
	"github.com/atomix/go-client/pkg/client/meta"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-framework/pkg/atomix/client"
	valueclient "github.com/atomix/go-framework/pkg/atomix/client/value"
	"google.golang.org/grpc"
)

// Type is the counter type
const Type = primitive.Type(valueclient.PrimitiveType)

// Client provides an API for creating Values
type Client interface {
	// GetValue gets the Value instance of the given name
	GetValue(ctx context.Context, name string, opts ...Option) (Value, error)
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
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (Value, error) {
	options := applyOptions(opts...)
	v := &value{
		client: valueclient.NewClient(client.ID(options.clientID), name, conn),
	}
	if err := v.create(ctx); err != nil {
		return nil, err
	}
	return v, nil
}

// value is the single partition implementation of Lock
type value struct {
	client valueclient.Client
}

func (v *value) Type() primitive.Type {
	return Type
}

func (v *value) Name() string {
	return v.client.Name()
}

func (v *value) Set(ctx context.Context, value []byte, opts ...SetOption) (meta.ObjectMeta, error) {
	input := &api.SetInput{
		Value: &api.Value{
			Value: value,
		},
	}
	for i := range opts {
		opts[i].beforeSet(input)
	}
	output, err := v.client.Set(ctx, input)
	if err != nil {
		return meta.ObjectMeta{}, err
	}
	for i := range opts {
		opts[i].afterSet(output)
	}
	return meta.New(output.Meta), nil
}

func (v *value) Get(ctx context.Context) ([]byte, meta.ObjectMeta, error) {
	input := &api.GetInput{}
	output, err := v.client.Get(ctx, input)
	if err != nil {
		return nil, meta.ObjectMeta{}, err
	}
	return output.Value.Value, meta.New(output.Value.Meta), nil
}

func (v *value) Watch(ctx context.Context, ch chan<- Event) error {
	input := &api.EventsInput{}
	outputCh := make(chan api.EventsOutput)
	if err := v.client.Events(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
		for output := range outputCh {
			switch output.Type {
			case api.EventsOutput_UPDATE:
				ch <- Event{
					ObjectMeta: meta.New(output.Value.Meta),
					Type:       EventUpdate,
					Value:      output.Value.Value,
				}
			}
		}
	}()
	return nil
}

func (v *value) create(ctx context.Context) error {
	return v.client.Create(ctx)
}

func (v *value) Close(ctx context.Context) error {
	return v.client.Close(ctx)
}

func (v *value) Delete(ctx context.Context) error {
	return v.client.Delete(ctx)
}
