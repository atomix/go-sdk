// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"context"
	"encoding/base64"
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	listv1 "github.com/atomix/runtime/api/atomix/list/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger()

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

func Client[E any](conn *grpc.ClientConn) primitive.Client[List[E], Option[E]] {
	return primitive.NewClient[List[E], Option[E]](newManager(conn), func(primitive *primitive.ManagedPrimitive, opts ...Option[E]) (List[E], error) {
		var options Options[E]
		for _, opt := range opts {
			opt.apply(&options)
		}
		if options.ElementType == nil {
			stringType := generic.Bytes()
			if elementType, ok := stringType.(generic.Type[E]); ok {
				options.ElementType = elementType
			} else {
				return nil, errors.NewInvalid("must configure a generic type for key parameter")
			}
		}
		return &typedList[E]{
			ManagedPrimitive: primitive,
			client:           listv1.NewListClient(conn),
			elementType:      options.ElementType,
		}, nil
	})
}

// list is the single partition implementation of List
type typedList[E any] struct {
	*primitive.ManagedPrimitive
	client      listv1.ListClient
	elementType generic.Type[E]
}

func (l *typedList[E]) Append(ctx context.Context, value E) error {
	bytes, err := l.elementType.Marshal(&value)
	if err != nil {
		return errors.NewInvalid("element encoding failed", err)
	}
	request := &listv1.AppendRequest{
		Headers: l.GetHeaders(),
		AppendInput: listv1.AppendInput{
			Value: listv1.Value{
				Value: base64.StdEncoding.EncodeToString(bytes),
			},
		},
	}
	_, err = l.client.Append(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (l *typedList[E]) Insert(ctx context.Context, index int, value E) error {
	bytes, err := l.elementType.Marshal(&value)
	if err != nil {
		return errors.NewInvalid("element encoding failed", err)
	}
	request := &listv1.InsertRequest{
		Headers: l.GetHeaders(),
		InsertInput: listv1.InsertInput{
			Item: listv1.Item{
				Index: uint32(index),
				Value: listv1.Value{
					Value: base64.StdEncoding.EncodeToString(bytes),
				},
			},
		},
	}
	_, err = l.client.Insert(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (l *typedList[E]) Set(ctx context.Context, index int, value E) error {
	bytes, err := l.elementType.Marshal(&value)
	if err != nil {
		return errors.NewInvalid("element encoding failed", err)
	}
	request := &listv1.SetRequest{
		Headers: l.GetHeaders(),
		SetInput: listv1.SetInput{
			Item: listv1.Item{
				Index: uint32(index),
				Value: listv1.Value{
					Value: base64.StdEncoding.EncodeToString(bytes),
				},
			},
		},
	}
	_, err = l.client.Set(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (l *typedList[E]) Get(ctx context.Context, index int) (E, error) {
	request := &listv1.GetRequest{
		Headers: l.GetHeaders(),
		GetInput: listv1.GetInput{
			Index: uint32(index),
		},
	}
	var e E
	response, err := l.client.Get(ctx, request)
	if err != nil {
		return e, errors.FromProto(err)
	}
	bytes, err := base64.StdEncoding.DecodeString(response.Item.Value.Value)
	if err != nil {
		return e, errors.NewInvalid("element decoding failed", err)
	}
	var elem E
	if err := l.elementType.Unmarshal(bytes, &elem); err != nil {
		return elem, err
	}
	return elem, nil
}

func (l *typedList[E]) Remove(ctx context.Context, index int) (E, error) {
	request := &listv1.RemoveRequest{
		Headers: l.GetHeaders(),
		RemoveInput: listv1.RemoveInput{
			Index: uint32(index),
		},
	}
	var e E
	response, err := l.client.Remove(ctx, request)
	if err != nil {
		return e, errors.FromProto(err)
	}
	bytes, err := base64.StdEncoding.DecodeString(response.Item.Value.Value)
	if err != nil {
		return e, errors.NewInvalid("element decoding failed", err)
	}
	var elem E
	if err := l.elementType.Unmarshal(bytes, &elem); err != nil {
		return elem, err
	}
	return elem, nil
}

func (l *typedList[E]) Len(ctx context.Context) (int, error) {
	request := &listv1.SizeRequest{
		Headers: l.GetHeaders(),
	}
	response, err := l.client.Size(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return int(response.Size_), nil
}

func (l *typedList[E]) Items(ctx context.Context, ch chan<- E) error {
	request := &listv1.ElementsRequest{
		Headers: l.GetHeaders(),
	}
	stream, err := l.client.Elements(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}

	go func() {
		defer close(ch)
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
				log.Errorf("Entries failed: %v", err)
				return
			}

			bytes, err := base64.StdEncoding.DecodeString(response.Item.Value.Value)
			if err != nil {
				log.Errorf("Failed to decode list item: %v", err)
			} else {
				var elem E
				if err := l.elementType.Unmarshal(bytes, &elem); err != nil {
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
	request := &listv1.EventsRequest{
		Headers: l.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeWatch(request)
	}

	stream, err := l.client.Events(ctx, request)
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
			for i := range opts {
				opts[i].afterWatch(response)
			}

			bytes, err := base64.StdEncoding.DecodeString(response.Event.Item.Value.Value)
			if err != nil {
				log.Errorf("Failed to decode list item: %v", err)
			} else {
				var elem E
				if err := l.elementType.Unmarshal(bytes, &elem); err != nil {
					log.Error(err)
					continue
				}

				switch response.Event.Type {
				case listv1.Event_ADD:
					ch <- Event[E]{
						Type:  EventAdd,
						Index: int(response.Event.Item.Index),
						Value: elem,
					}
				case listv1.Event_REMOVE:
					ch <- Event[E]{
						Type:  EventRemove,
						Index: int(response.Event.Item.Index),
						Value: elem,
					}
				case listv1.Event_REPLAY:
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
	request := &listv1.ClearRequest{
		Headers: l.GetHeaders(),
	}
	_, err := l.client.Clear(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}
