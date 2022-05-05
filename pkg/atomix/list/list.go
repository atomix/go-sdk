// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"context"
	"encoding/base64"
	api "github.com/atomix/atomix-api/go/atomix/primitive/list"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"github.com/atomix/go-client/pkg/atomix/primitive/codec"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger("atomix", "client", "list")

// Type is the list type
const Type primitive.Type = "List"

// List provides a distributed list data structure
// The list values are defines as strings. To store more complex types in the list, encode values to strings e.g.
// using base 64 encoding.
type List[E any] interface {
	primitive.Primitive

	// Append pushes a value on to the end of the list
	Append(ctx context.Context, value E) error

	// Insert inserts a value at the given index
	Insert(ctx context.Context, index int, value E) error

	// Set sets the value at the given index
	Set(ctx context.Context, index int, value E) error

	// Get gets the value at the given index
	Get(ctx context.Context, index int) (E, error)

	// Remove removes and returns the value at the given index
	Remove(ctx context.Context, index int) (E, error)

	// Len gets the length of the list
	Len(ctx context.Context) (int, error)

	// Items iterates through the values in the list
	// This is a non-blocking method. If the method returns without error, values will be pushed on to the
	// given channel and the channel will be closed once all values have been read from the list.
	Items(ctx context.Context, ch chan<- E) error

	// Watch watches the list for changes
	// This is a non-blocking method. If the method returns without error, list events will be pushed onto
	// the given channel.
	Watch(ctx context.Context, ch chan<- Event[E], opts ...WatchOption) error

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
type Event[E any] struct {
	// Type indicates the event type
	Type EventType

	// Index is the index at which the event occurred
	Index int

	// Value is the value that was changed
	Value E
}

// New creates a new list primitive
func New[E any](ctx context.Context, name string, conn *grpc.ClientConn, opts ...primitive.Option) (List[E], error) {
	options := newListOptions[E]{}
	for _, opt := range opts {
		if op, ok := opt.(Option[E]); ok {
			op.applyNewList(&options)
		}
	}
	if options.elementCodec == nil {
		options.elementCodec = codec.String().(codec.Codec[E])
	}
	l := &typedList[E]{
		Client: primitive.NewClient(Type, name, conn, opts...),
		client: api.NewListServiceClient(conn),
		codec:  options.elementCodec,
	}
	if err := l.Create(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// list is the single partition implementation of List
type typedList[E any] struct {
	*primitive.Client
	client api.ListServiceClient
	codec  codec.Codec[E]
}

func (l *typedList[E]) Append(ctx context.Context, value E) error {
	bytes, err := l.codec.Encode(value)
	if err != nil {
		return errors.NewInvalid("element encoding failed", err)
	}
	request := &api.AppendRequest{
		Headers: l.GetHeaders(),
		Value: api.Value{
			Value: base64.StdEncoding.EncodeToString(bytes),
		},
	}
	_, err = l.client.Append(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (l *typedList[E]) Insert(ctx context.Context, index int, value E) error {
	bytes, err := l.codec.Encode(value)
	if err != nil {
		return errors.NewInvalid("element encoding failed", err)
	}
	request := &api.InsertRequest{
		Headers: l.GetHeaders(),
		Item: api.Item{
			Index: uint32(index),
			Value: api.Value{
				Value: base64.StdEncoding.EncodeToString(bytes),
			},
		},
	}
	_, err = l.client.Insert(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (l *typedList[E]) Set(ctx context.Context, index int, value E) error {
	bytes, err := l.codec.Encode(value)
	if err != nil {
		return errors.NewInvalid("element encoding failed", err)
	}
	request := &api.SetRequest{
		Headers: l.GetHeaders(),
		Item: api.Item{
			Index: uint32(index),
			Value: api.Value{
				Value: base64.StdEncoding.EncodeToString(bytes),
			},
		},
	}
	_, err = l.client.Set(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (l *typedList[E]) Get(ctx context.Context, index int) (E, error) {
	request := &api.GetRequest{
		Headers: l.GetHeaders(),
		Index:   uint32(index),
	}
	var e E
	response, err := l.client.Get(ctx, request)
	if err != nil {
		return e, errors.From(err)
	}
	bytes, err := base64.StdEncoding.DecodeString(response.Item.Value.Value)
	if err != nil {
		return e, errors.NewInvalid("element decoding failed", err)
	}
	return l.codec.Decode(bytes)
}

func (l *typedList[E]) Remove(ctx context.Context, index int) (E, error) {
	request := &api.RemoveRequest{
		Headers: l.GetHeaders(),
		Index:   uint32(index),
	}
	var e E
	response, err := l.client.Remove(ctx, request)
	if err != nil {
		return e, errors.From(err)
	}
	bytes, err := base64.StdEncoding.DecodeString(response.Item.Value.Value)
	if err != nil {
		return e, errors.NewInvalid("element decoding failed", err)
	}
	return l.codec.Decode(bytes)
}

func (l *typedList[E]) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Headers: l.GetHeaders(),
	}
	response, err := l.client.Size(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return int(response.Size_), nil
}

func (l *typedList[E]) Items(ctx context.Context, ch chan<- E) error {
	request := &api.ElementsRequest{
		Headers: l.GetHeaders(),
	}
	stream, err := l.client.Elements(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	go func() {
		defer close(ch)
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
				log.Errorf("Entries failed: %v", err)
				return
			}

			bytes, err := base64.StdEncoding.DecodeString(response.Item.Value.Value)
			if err != nil {
				log.Errorf("Failed to decode list item: %v", err)
			} else {
				elem, err := l.codec.Decode(bytes)
				if err != nil {
					log.Error(err)
				} else {
					ch <- elem
				}
			}
		}
	}()
	return nil
}

func (l *typedList[E]) Watch(ctx context.Context, ch chan<- Event[E], opts ...WatchOption) error {
	request := &api.EventsRequest{
		Headers: l.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeWatch(request)
	}

	stream, err := l.client.Events(ctx, request)
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
			for i := range opts {
				opts[i].afterWatch(response)
			}

			bytes, err := base64.StdEncoding.DecodeString(response.Event.Item.Value.Value)
			if err != nil {
				log.Errorf("Failed to decode list item: %v", err)
			} else {
				elem, err := l.codec.Decode(bytes)
				if err != nil {
					log.Error(err)
					continue
				}

				switch response.Event.Type {
				case api.Event_ADD:
					ch <- Event[E]{
						Type:  EventAdd,
						Index: int(response.Event.Item.Index),
						Value: elem,
					}
				case api.Event_REMOVE:
					ch <- Event[E]{
						Type:  EventRemove,
						Index: int(response.Event.Item.Index),
						Value: elem,
					}
				case api.Event_REPLAY:
					ch <- Event[E]{
						Type:  EventReplay,
						Index: int(response.Event.Item.Index),
						Value: elem,
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

func (l *typedList[E]) Clear(ctx context.Context) error {
	request := &api.ClearRequest{
		Headers: l.GetHeaders(),
	}
	_, err := l.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}
