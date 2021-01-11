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

package list

import (
	"context"
	"encoding/base64"
	api "github.com/atomix/api/go/atomix/primitive/list"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-framework/pkg/atomix/client"
	listclient "github.com/atomix/go-framework/pkg/atomix/client/list"
	"google.golang.org/grpc"
)

// Type is the counter type
const Type = primitive.Type(listclient.PrimitiveType)

// Client provides an API for creating Lists
type Client interface {
	// GetList gets the List instance of the given name
	GetList(ctx context.Context, name string, opts ...Option) (List, error)
}

// List provides a distributed list data structure
// The list values are defines as strings. To store more complex types in the list, encode values to strings e.g.
// using base 64 encoding.
type List interface {
	primitive.Primitive

	// Append pushes a value on to the end of the list
	Append(ctx context.Context, value []byte) error

	// Insert inserts a value at the given index
	Insert(ctx context.Context, index int, value []byte) error

	// Set sets the value at the given index
	Set(ctx context.Context, index int, value []byte) error

	// Get gets the value at the given index
	Get(ctx context.Context, index int) ([]byte, error)

	// Remove removes and returns the value at the given index
	Remove(ctx context.Context, index int) ([]byte, error)

	// Len gets the length of the list
	Len(ctx context.Context) (int, error)

	// Items iterates through the values in the list
	// This is a non-blocking method. If the method returns without error, values will be pushed on to the
	// given channel and the channel will be closed once all values have been read from the list.
	Items(ctx context.Context, ch chan<- []byte) error

	// Watch watches the list for changes
	// This is a non-blocking method. If the method returns without error, list events will be pushed onto
	// the given channel.
	Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error

	// Clear removes all values from the list
	Clear(ctx context.Context) error
}

// EventType is the type for a list Event
type EventType string

const (
	// EventAdd indicates a value was added to the list
	EventAdd EventType = "add"

	// EventRemove indicates a value was removed from the list
	EventRemove EventType = "remove"

	// EventReplay indicates a value was replayed
	EventReplay EventType = "replay"
)

// Event is a list change event
type Event struct {
	// Type indicates the event type
	Type EventType

	// Index is the index at which the event occurred
	Index int

	// Value is the value that was changed
	Value []byte
}

// New creates a new list primitive
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (List, error) {
	options := applyOptions(opts...)
	l := &list{
		client: listclient.NewClient(client.ID(options.clientID), name, conn),
	}
	if err := l.create(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// list is the single partition implementation of List
type list struct {
	client listclient.Client
}

func (l *list) Type() primitive.Type {
	return Type
}

func (l *list) Name() string {
	return l.client.Name()
}

func (l *list) Append(ctx context.Context, value []byte) error {
	input := &api.AppendInput{
		Value: base64.StdEncoding.EncodeToString(value),
	}
	_, err := l.client.Append(ctx, input)
	return err
}

func (l *list) Insert(ctx context.Context, index int, value []byte) error {
	input := &api.InsertInput{
		Index: uint32(index),
		Value: base64.StdEncoding.EncodeToString(value),
	}
	_, err := l.client.Insert(ctx, input)
	return err
}

func (l *list) Set(ctx context.Context, index int, value []byte) error {
	input := &api.SetInput{
		Index: uint32(index),
		Value: base64.StdEncoding.EncodeToString(value),
	}
	_, err := l.client.Set(ctx, input)
	return err
}

func (l *list) Get(ctx context.Context, index int) ([]byte, error) {
	input := &api.GetInput{
		Index: uint32(index),
	}
	output, err := l.client.Get(ctx, input)
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(output.Value)
}

func (l *list) Remove(ctx context.Context, index int) ([]byte, error) {
	input := &api.RemoveInput{
		Index: uint32(index),
	}
	output, err := l.client.Remove(ctx, input)
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(output.Value)
}

func (l *list) Len(ctx context.Context) (int, error) {
	output, err := l.client.Size(ctx)
	if err != nil {
		return 0, err
	}
	return int(output.Size_), nil
}

func (l *list) Items(ctx context.Context, ch chan<- []byte) error {
	input := &api.ElementsInput{}
	outputCh := make(chan api.ElementsOutput)
	if err := l.client.Elements(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
		for output := range outputCh {
			if bytes, err := base64.StdEncoding.DecodeString(output.Value); err == nil {
				ch <- bytes
			}
		}
	}()
	return nil
}

func (l *list) Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error {
	input := &api.EventsInput{}
	for _, opt := range opts {
		opt.beforeWatch(input)
	}
	outputCh := make(chan api.EventsOutput)
	if err := l.client.Events(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
		for output := range outputCh {
			var t EventType
			switch output.Type {
			case api.EventsOutput_ADD:
				t = EventAdd
			case api.EventsOutput_REMOVE:
				t = EventRemove
			case api.EventsOutput_REPLAY:
				t = EventReplay
			}
			if bytes, err := base64.StdEncoding.DecodeString(output.Value); err == nil {
				ch <- Event{
					Type:  t,
					Index: int(output.Index),
					Value: bytes,
				}
			}
		}
	}()
	return nil
}

func (l *list) Clear(ctx context.Context) error {
	return l.client.Clear(ctx)
}

func (l *list) create(ctx context.Context) error {
	return l.client.Create(ctx)
}

func (l *list) Close(ctx context.Context) error {
	return l.client.Close(ctx)
}

func (l *list) Delete(ctx context.Context) error {
	return l.client.Delete(ctx)
}
