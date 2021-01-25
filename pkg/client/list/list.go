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
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger("atomix", "client", "list")

// Type is the list type
const Type primitive.Type = "List"

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
	l := &list{
		Client: primitive.NewClient(Type, name, conn),
		client: api.NewListServiceClient(conn),
	}
	if err := l.Create(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// list is the single partition implementation of List
type list struct {
	*primitive.Client
	client api.ListServiceClient
}

func (l *list) Append(ctx context.Context, value []byte) error {
	request := &api.AppendRequest{
		Headers: l.GetHeaders(),
		Value: api.Value{
			Value: base64.StdEncoding.EncodeToString(value),
		},
	}
	_, err := l.client.Append(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (l *list) Insert(ctx context.Context, index int, value []byte) error {
	request := &api.InsertRequest{
		Headers: l.GetHeaders(),
		Item: api.Item{
			Index: uint32(index),
			Value: api.Value{
				Value: base64.StdEncoding.EncodeToString(value),
			},
		},
	}
	_, err := l.client.Insert(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (l *list) Set(ctx context.Context, index int, value []byte) error {
	request := &api.SetRequest{
		Headers: l.GetHeaders(),
		Item: api.Item{
			Index: uint32(index),
			Value: api.Value{
				Value: base64.StdEncoding.EncodeToString(value),
			},
		},
	}
	_, err := l.client.Set(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (l *list) Get(ctx context.Context, index int) ([]byte, error) {
	request := &api.GetRequest{
		Headers: l.GetHeaders(),
		Index:   uint32(index),
	}
	response, err := l.client.Get(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return base64.StdEncoding.DecodeString(response.Item.Value.Value)
}

func (l *list) Remove(ctx context.Context, index int) ([]byte, error) {
	request := &api.RemoveRequest{
		Headers: l.GetHeaders(),
		Index:   uint32(index),
	}
	response, err := l.client.Remove(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return base64.StdEncoding.DecodeString(response.Item.Value.Value)
}

func (l *list) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Headers: l.GetHeaders(),
	}
	response, err := l.client.Size(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return int(response.Size_), nil
}

func (l *list) Items(ctx context.Context, ch chan<- []byte) error {
	request := &api.ElementsRequest{
		Headers: l.GetHeaders(),
	}
	stream, err := l.client.Elements(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	openCh := make(chan struct{})
	go func() {
		defer close(ch)
		open := false
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				log.Errorf("Entries failed: %v", err)
			} else {
				if !open {
					close(openCh)
					open = true
				}
				bytes, err := base64.StdEncoding.DecodeString(response.Item.Value.Value)
				if err != nil {
					log.Errorf("Failed to decode list item: %v", err)
				} else {
					ch <- bytes
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

func (l *list) Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error {
	request := &api.EventsRequest{
		Headers: l.GetHeaders(),
	}
	stream, err := l.client.Events(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	openCh := make(chan struct{})
	go func() {
		defer close(ch)
		open := false
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				log.Errorf("Watch failed: %v", err)
			} else {
				bytes, err := base64.StdEncoding.DecodeString(response.Event.Item.Value.Value)
				if err != nil {
					log.Errorf("Failed to decode list item: %v", err)
				} else {
					if !open {
						close(openCh)
						open = true
					}
					switch response.Event.Type {
					case api.Event_ADD:
						ch <- Event{
							Type:  EventAdd,
							Index: int(response.Event.Item.Index),
							Value: bytes,
						}
					case api.Event_REMOVE:
						ch <- Event{
							Type:  EventRemove,
							Index: int(response.Event.Item.Index),
							Value: bytes,
						}
					case api.Event_REPLAY:
						ch <- Event{
							Type:  EventReplay,
							Index: int(response.Event.Item.Index),
							Value: bytes,
						}
					}
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

func (l *list) Clear(ctx context.Context) error {
	request := &api.ClearRequest{
		Headers: l.GetHeaders(),
	}
	_, err := l.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}
