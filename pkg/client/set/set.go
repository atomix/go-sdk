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

package set

import (
	"context"
	api "github.com/atomix/api/go/atomix/primitive/set"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-framework/pkg/atomix/client"
	setclient "github.com/atomix/go-framework/pkg/atomix/client/set"
	"google.golang.org/grpc"
)

// Type is the counter type
const Type = primitive.Type(setclient.PrimitiveType)

// Client provides an API for creating Sets
type Client interface {
	// GetSet gets the Set instance of the given name
	GetSet(ctx context.Context, name string, opts ...Option) (Set, error)
}

// Set provides a distributed set data structure
// The set values are defines as strings. To store more complex types in the set, encode values to strings e.g.
// using base 64 encoding.
type Set interface {
	primitive.Primitive

	// Add adds a value to the set
	Add(ctx context.Context, value string) (bool, error)

	// Remove removes a value from the set
	// A bool indicating whether the set contained the given value will be returned
	Remove(ctx context.Context, value string) (bool, error)

	// Contains returns a bool indicating whether the set contains the given value
	Contains(ctx context.Context, value string) (bool, error)

	// Len gets the set size in number of elements
	Len(ctx context.Context) (int, error)

	// Clear removes all values from the set
	Clear(ctx context.Context) error

	// Elements lists the elements in the set
	Elements(ctx context.Context, ch chan<- string) error

	// Watch watches the set for changes
	// This is a non-blocking method. If the method returns without error, set events will be pushed onto
	// the given channel.
	Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error
}

// EventType is the type of a set event
type EventType string

const (
	// EventAdd indicates a value was added to the set
	EventAdd EventType = "add"

	// EventRemove indicates a value was removed from the set
	EventRemove EventType = "remove"

	// EventReplay indicates a value was replayed
	EventReplay EventType = "replay"
)

// Event is a set change event
type Event struct {
	// Type is the change event type
	Type EventType

	// Value is the value that changed
	Value string
}

// New creates a new partitioned set primitive
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (Set, error) {
	options := applyOptions(opts...)
	s := &set{
		client: setclient.NewClient(client.ID(options.clientID), name, conn),
	}
	if err := s.create(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

type set struct {
	client setclient.Client
}

func (s *set) Type() primitive.Type {
	return Type
}

func (s *set) Name() string {
	return s.client.Name()
}

func (s *set) Add(ctx context.Context, value string) (bool, error) {
	input := &api.AddInput{
		Value: value,
	}
	output, err := s.client.Add(ctx, input)
	if err != nil {
		return false, err
	}
	return output.Added, nil
}

func (s *set) Remove(ctx context.Context, value string) (bool, error) {
	input := &api.RemoveInput{
		Value: value,
	}
	output, err := s.client.Remove(ctx, input)
	if err != nil {
		return false, err
	}
	return output.Removed, nil
}

func (s *set) Contains(ctx context.Context, value string) (bool, error) {
	input := &api.ContainsInput{
		Value: value,
	}
	output, err := s.client.Contains(ctx, input)
	if err != nil {
		return false, err
	}
	return output.Contains, nil
}

func (s *set) Len(ctx context.Context) (int, error) {
	output, err := s.client.Size(ctx)
	if err != nil {
		return 0, err
	}
	return int(output.Size_), nil
}

func (s *set) Clear(ctx context.Context) error {
	return s.client.Clear(ctx)
}

func (s *set) Elements(ctx context.Context, ch chan<- string) error {
	input := &api.ElementsInput{}
	outputCh := make(chan api.ElementsOutput)
	if err := s.client.Elements(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
		defer close(ch)
		for output := range outputCh {
			ch <- output.Value
		}
	}()
	return nil
}

func (s *set) Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error {
	input := &api.EventsInput{}
	for _, opt := range opts {
		opt.beforeWatch(input)
	}
	outputCh := make(chan api.EventsOutput)
	if err := s.client.Events(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
		defer close(ch)
		for output := range outputCh {
			var eventType EventType
			switch output.Type {
			case api.EventsOutput_ADD:
				eventType = EventAdd
			case api.EventsOutput_REMOVE:
				eventType = EventRemove
			case api.EventsOutput_REPLAY:
				eventType = EventReplay
			}
			ch <- Event{
				Type:  eventType,
				Value: output.Value,
			}
		}
	}()
	return nil
}

func (s *set) create(ctx context.Context) error {
	return s.client.Create(ctx)
}

func (s *set) Close(ctx context.Context) error {
	return s.client.Close(ctx)
}

func (s *set) Delete(ctx context.Context) error {
	return s.client.Delete(ctx)
}
