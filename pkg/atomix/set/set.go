// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	"encoding/base64"
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	setv1 "github.com/atomix/runtime/api/atomix/runtime/set/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"io"
)

var log = logging.GetLogger()

// Set provides a distributed set data structure
// The set values are defines as strings. To store more complex types in the set, encode values to strings e.g.
// using base 64 encoding.
type Set[E any] interface {
	primitive.Primitive

	// Add adds a value to the set
	Add(ctx context.Context, value E) (bool, error)

	// Remove removes a value from the set
	// A bool indicating whether the set contained the given value will be returned
	Remove(ctx context.Context, value E) (bool, error)

	// Contains returns a bool indicating whether the set contains the given value
	Contains(ctx context.Context, value E) (bool, error)

	// Len gets the set size in number of elements
	Len(ctx context.Context) (int, error)

	// Clear removes all values from the set
	Clear(ctx context.Context) error

	// Elements lists the elements in the set
	Elements(ctx context.Context, ch chan<- E) error

	// Watch watches the set for changes
	// This is a non-blocking method. If the method returns without error, set events will be pushed onto
	// the given channel.
	Watch(ctx context.Context, ch chan<- Event[E], opts ...WatchOption) error
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
type Event[E any] struct {
	// Type is the change event type
	Type EventType

	// Value is the value that changed
	Value E
}

func New[E any](client setv1.SetClient) func(context.Context, string, ...Option[E]) (Set[E], error) {
	return func(ctx context.Context, name string, opts ...Option[E]) (Set[E], error) {
		var options Options[E]
		options.Apply(opts...)
		if options.ElementType == nil {
			stringType := generic.Bytes()
			if elementType, ok := stringType.(generic.Type[E]); ok {
				options.ElementType = elementType
			} else {
				return nil, errors.NewInvalid("must configure a generic type for element parameter")
			}
		}
		indexedMap := &setPrimitive[E]{
			Primitive:   primitive.New(name),
			client:      client,
			elementType: options.ElementType,
		}
		if err := indexedMap.create(ctx, options.Tags); err != nil {
			return nil, err
		}
		return indexedMap, nil
	}
}

type setPrimitive[E any] struct {
	primitive.Primitive
	client      setv1.SetClient
	elementType generic.Type[E]
}

func (s *setPrimitive[E]) Add(ctx context.Context, value E) (bool, error) {
	bytes, err := s.elementType.Marshal(&value)
	if err != nil {
		return false, errors.NewInvalid("element encoding failed", err)
	}
	request := &setv1.AddRequest{
		ID: runtimev1.PrimitiveId{
			Name: s.Name(),
		},
		Element: setv1.Element{
			Value: base64.StdEncoding.EncodeToString(bytes),
		},
	}
	_, err = s.client.Add(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if errors.IsAlreadyExists(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *setPrimitive[E]) Remove(ctx context.Context, value E) (bool, error) {
	bytes, err := s.elementType.Marshal(&value)
	if err != nil {
		return false, errors.NewInvalid("element encoding failed", err)
	}
	request := &setv1.RemoveRequest{
		ID: runtimev1.PrimitiveId{
			Name: s.Name(),
		},
		Element: setv1.Element{
			Value: base64.StdEncoding.EncodeToString(bytes),
		},
	}
	_, err = s.client.Remove(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *setPrimitive[E]) Contains(ctx context.Context, value E) (bool, error) {
	bytes, err := s.elementType.Marshal(&value)
	if err != nil {
		return false, errors.NewInvalid("element encoding failed", err)
	}
	request := &setv1.ContainsRequest{
		ID: runtimev1.PrimitiveId{
			Name: s.Name(),
		},
		Element: setv1.Element{
			Value: base64.StdEncoding.EncodeToString(bytes),
		},
	}
	response, err := s.client.Contains(ctx, request)
	if err != nil {
		return false, errors.FromProto(err)
	}
	return response.Contains, nil
}

func (s *setPrimitive[E]) Len(ctx context.Context) (int, error) {
	request := &setv1.SizeRequest{
		ID: runtimev1.PrimitiveId{
			Name: s.Name(),
		},
	}
	response, err := s.client.Size(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return int(response.Size_), nil
}

func (s *setPrimitive[E]) Clear(ctx context.Context) error {
	request := &setv1.ClearRequest{
		ID: runtimev1.PrimitiveId{
			Name: s.Name(),
		},
	}
	_, err := s.client.Clear(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (s *setPrimitive[E]) Elements(ctx context.Context, ch chan<- E) error {
	request := &setv1.ElementsRequest{
		ID: runtimev1.PrimitiveId{
			Name: s.Name(),
		},
	}
	stream, err := s.client.Elements(ctx, request)
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
				log.Errorf("Elements failed: %v", err)
				return
			}

			bytes, err := base64.StdEncoding.DecodeString(response.Element.Value)
			if err != nil {
				log.Errorf("Failed to decode list item: %v", err)
			} else {
				var elem E
				if err := s.elementType.Unmarshal(bytes, &elem); err != nil {
					log.Error(err)
				} else {
					ch <- elem
				}
			}
		}
	}()
	return nil
}

func (s *setPrimitive[E]) Watch(ctx context.Context, ch chan<- Event[E], opts ...WatchOption) error {
	request := &setv1.EventsRequest{
		ID: runtimev1.PrimitiveId{
			Name: s.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeWatch(request)
	}

	stream, err := s.client.Events(ctx, request)
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

			bytes, err := base64.StdEncoding.DecodeString(response.Event.Element.Value)
			if err != nil {
				log.Errorf("Failed to decode list item: %v", err)
			} else {
				var elem E
				if err := s.elementType.Unmarshal(bytes, &elem); err != nil {
					log.Error(err)
					continue
				}

				switch response.Event.Type {
				case setv1.Event_ADD:
					ch <- Event[E]{
						Type:  EventAdd,
						Value: elem,
					}
				case setv1.Event_REMOVE:
					ch <- Event[E]{
						Type:  EventRemove,
						Value: elem,
					}
				case setv1.Event_REPLAY:
					ch <- Event[E]{
						Type:  EventReplay,
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

func (s *setPrimitive[E]) create(ctx context.Context, tags map[string]string) error {
	request := &setv1.CreateRequest{
		ID: runtimev1.PrimitiveId{
			Name: s.Name(),
		},
		Tags: tags,
	}
	_, err := s.client.Create(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (s *setPrimitive[E]) Close(ctx context.Context) error {
	request := &setv1.CloseRequest{
		ID: runtimev1.PrimitiveId{
			Name: s.Name(),
		},
	}
	_, err := s.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
