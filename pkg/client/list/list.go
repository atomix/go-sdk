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
	"errors"
	api "github.com/atomix/atomix-api/proto/atomix/list"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

// Type is the list type
const Type primitive.Type = "list"

// Client provides an API for creating Lists
type Client interface {
	// GetList gets the List instance of the given name
	GetList(ctx context.Context, name string, opts ...session.Option) (List, error)
}

// List provides a distributed list data structure
// The list values are defines as strings. To store more complex types in the list, encode values to strings e.g.
// using base 64 encoding.
type List interface {
	primitive.Primitive

	// Append pushes a value on to the end of the list
	Append(ctx context.Context, value string) error

	// Insert inserts a value at the given index
	Insert(ctx context.Context, index int, value string) error

	// Set sets the value at the given index
	Set(ctx context.Context, index int, value string) error

	// Get gets the value at the given index
	Get(ctx context.Context, index int) (string, error)

	// Remove removes and returns the value at the given index
	Remove(ctx context.Context, index int) (string, error)

	// Len gets the length of the list
	Len(ctx context.Context) (int, error)

	// Items iterates through the values in the list
	// This is a non-blocking method. If the method returns without error, values will be pushed on to the
	// given channel and the channel will be closed once all values have been read from the list.
	Items(ctx context.Context, ch chan<- string) error

	// Watch watches the list for changes
	// This is a non-blocking method. If the method returns without error, list events will be pushed onto
	// the given channel.
	Watch(ctx context.Context, ch chan<- *Event, opts ...WatchOption) error

	// Clear removes all values from the list
	Clear(ctx context.Context) error
}

// EventType is the type for a list Event
type EventType string

const (
	// EventNone indicates the event is not a change event
	EventNone EventType = ""

	// EventInserted indicates a value was added to the list
	EventInserted EventType = "added"

	// EventRemoved indicates a value was removed from the list
	EventRemoved EventType = "removed"
)

// Event is a list change event
type Event struct {
	// Type indicates the event type
	Type EventType

	// Index is the index at which the event occurred
	Index int

	// Value is the value that was changed
	Value string
}

// New creates a new list primitive
func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.Option) (List, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}
	return newList(ctx, name, partitions[i], opts...)
}

// newList creates a new list for the given partition
func newList(ctx context.Context, name primitive.Name, conn *grpc.ClientConn, opts ...session.Option) (*list, error) {
	client := api.NewListServiceClient(conn)
	sess, err := session.New(ctx, name, &sessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &list{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

// list is the single partition implementation of List
type list struct {
	name    primitive.Name
	client  api.ListServiceClient
	session *session.Session
}

func (l *list) Name() primitive.Name {
	return l.name
}

func (l *list) Append(ctx context.Context, value string) error {
	stream, header := l.session.NextStream()
	defer stream.Close()

	request := &api.AppendRequest{
		Header: header,
		Value:  value,
	}

	response, err := l.client.Append(ctx, request)
	if err != nil {
		return err
	}

	l.session.RecordResponse(request.Header, response.Header)
	return err
}

func (l *list) Insert(ctx context.Context, index int, value string) error {
	stream, header := l.session.NextStream()
	defer stream.Close()

	request := &api.InsertRequest{
		Header: header,
		Index:  uint32(index),
		Value:  value,
	}

	response, err := l.client.Insert(ctx, request)
	if err != nil {
		return err
	}

	l.session.RecordResponse(request.Header, response.Header)

	switch response.Status {
	case api.ResponseStatus_OUT_OF_BOUNDS:
		return errors.New("index out of bounds")
	default:
		return nil
	}
}

func (l *list) Set(ctx context.Context, index int, value string) error {
	stream, header := l.session.NextStream()
	defer stream.Close()

	request := &api.SetRequest{
		Header: header,
		Index:  uint32(index),
		Value:  value,
	}

	response, err := l.client.Set(ctx, request)
	if err != nil {
		return err
	}

	l.session.RecordResponse(request.Header, response.Header)

	switch response.Status {
	case api.ResponseStatus_OUT_OF_BOUNDS:
		return errors.New("index out of bounds")
	default:
		return nil
	}
}

func (l *list) Get(ctx context.Context, index int) (string, error) {
	request := &api.GetRequest{
		Header: l.session.GetRequest(),
		Index:  uint32(index),
	}

	response, err := l.client.Get(ctx, request)
	if err != nil {
		return "", err
	}

	l.session.RecordResponse(request.Header, response.Header)

	switch response.Status {
	case api.ResponseStatus_OUT_OF_BOUNDS:
		return "", errors.New("index out of bounds")
	default:
		return response.Value, nil
	}
}

func (l *list) Remove(ctx context.Context, index int) (string, error) {
	stream, header := l.session.NextStream()
	defer stream.Close()

	request := &api.RemoveRequest{
		Header: header,
		Index:  uint32(index),
	}

	response, err := l.client.Remove(ctx, request)
	if err != nil {
		return "", err
	}

	l.session.RecordResponse(request.Header, response.Header)

	switch response.Status {
	case api.ResponseStatus_OUT_OF_BOUNDS:
		return "", errors.New("index out of bounds")
	default:
		return response.Value, nil
	}
}

func (l *list) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Header: l.session.GetRequest(),
	}

	response, err := l.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	l.session.RecordResponse(request.Header, response.Header)
	return int(response.Size_), nil
}

func (l *list) Items(ctx context.Context, ch chan<- string) error {
	request := &api.IterateRequest{
		Header: l.session.GetRequest(),
	}
	entries, err := l.client.Iterate(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		defer close(ch)
		for {
			response, err := entries.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				glog.Error("Failed to receive items stream", err)
				break
			}

			// Record the response header
			l.session.RecordResponse(request.Header, response.Header)

			ch <- response.Value
		}
	}()
	return nil
}

func (l *list) Watch(ctx context.Context, ch chan<- *Event, opts ...WatchOption) error {
	stream, header := l.session.NextStream()

	request := &api.EventRequest{
		Header: header,
	}

	for _, opt := range opts {
		opt.beforeWatch(request)
	}

	events, err := l.client.Events(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		defer close(ch)
		for {
			response, err := events.Recv()
			if err == io.EOF {
				stream.Close()
				break
			}

			if err != nil {
				glog.Error("Failed to receive event stream", err)
				stream.Close()
				break
			}

			for _, opt := range opts {
				opt.afterWatch(response)
			}

			// Record the response header
			l.session.RecordResponse(request.Header, response.Header)

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			var t EventType
			switch response.Type {
			case api.EventResponse_NONE:
				t = EventNone
			case api.EventResponse_ADDED:
				t = EventInserted
			case api.EventResponse_REMOVED:
				t = EventRemoved
			}

			ch <- &Event{
				Type:  t,
				Index: int(response.Index),
				Value: response.Value,
			}
		}
	}()
	return nil
}

func (l *list) Clear(ctx context.Context) error {
	stream, header := l.session.NextStream()
	defer stream.Close()

	request := &api.ClearRequest{
		Header: header,
	}

	response, err := l.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	l.session.RecordResponse(request.Header, response.Header)
	return nil
}

func (l *list) Close() error {
	return l.session.Close()
}

func (l *list) Delete() error {
	return l.session.Delete()
}
