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
	"encoding/base64"
	api "github.com/atomix/atomix-api/go/atomix/primitive/set"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive/codec"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger("atomix", "client", "set")

// Type is the set type
const Type primitive.Type = "Set"

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

// New creates a new partitioned set primitive
func New[E any](ctx context.Context, name string, conn *grpc.ClientConn, opts ...primitive.Option) (Set[E], error) {
	options := newSetOptions[E]{}
	for _, opt := range opts {
		if op, ok := opt.(Option[E]); ok {
			op.applyNewSet(&options)
		}
	}
	if options.elementCodec == nil {
		options.elementCodec = codec.String().(codec.Codec[E])
	}
	s := &typedSet[E]{
		Client: primitive.NewClient(Type, name, conn, opts...),
		client: api.NewSetServiceClient(conn),
		codec:  options.elementCodec,
	}
	if err := s.Create(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

type typedSet[E any] struct {
	*primitive.Client
	client api.SetServiceClient
	codec  codec.Codec[E]
}

func (s *typedSet[E]) Add(ctx context.Context, value E) (bool, error) {
	bytes, err := s.codec.Encode(value)
	if err != nil {
		return false, errors.NewInvalid("element encoding failed", err)
	}
	request := &api.AddRequest{
		Headers: s.GetHeaders(),
		Element: api.Element{
			Value: base64.StdEncoding.EncodeToString(bytes),
		},
	}
	_, err = s.client.Add(ctx, request)
	if err != nil {
		err = errors.From(err)
		if errors.IsAlreadyExists(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *typedSet[E]) Remove(ctx context.Context, value E) (bool, error) {
	bytes, err := s.codec.Encode(value)
	if err != nil {
		return false, errors.NewInvalid("element encoding failed", err)
	}
	request := &api.RemoveRequest{
		Headers: s.GetHeaders(),
		Element: api.Element{
			Value: base64.StdEncoding.EncodeToString(bytes),
		},
	}
	_, err = s.client.Remove(ctx, request)
	if err != nil {
		err = errors.From(err)
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *typedSet[E]) Contains(ctx context.Context, value E) (bool, error) {
	bytes, err := s.codec.Encode(value)
	if err != nil {
		return false, errors.NewInvalid("element encoding failed", err)
	}
	request := &api.ContainsRequest{
		Headers: s.GetHeaders(),
		Element: api.Element{
			Value: base64.StdEncoding.EncodeToString(bytes),
		},
	}
	response, err := s.client.Contains(ctx, request)
	if err != nil {
		return false, errors.From(err)
	}
	return response.Contains, nil
}

func (s *typedSet[E]) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Headers: s.GetHeaders(),
	}
	response, err := s.client.Size(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return int(response.Size_), nil
}

func (s *typedSet[E]) Clear(ctx context.Context) error {
	request := &api.ClearRequest{
		Headers: s.GetHeaders(),
	}
	_, err := s.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (s *typedSet[E]) Elements(ctx context.Context, ch chan<- E) error {
	request := &api.ElementsRequest{
		Headers: s.GetHeaders(),
	}
	stream, err := s.client.Elements(ctx, request)
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
				log.Errorf("Elements failed: %v", err)
				return
			}

			bytes, err := base64.StdEncoding.DecodeString(response.Element.Value)
			if err != nil {
				log.Errorf("Failed to decode list item: %v", err)
			} else {
				elem, err := s.codec.Decode(bytes)
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

func (s *typedSet[E]) Watch(ctx context.Context, ch chan<- Event[E], opts ...WatchOption) error {
	request := &api.EventsRequest{
		Headers: s.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeWatch(request)
	}

	stream, err := s.client.Events(ctx, request)
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

			bytes, err := base64.StdEncoding.DecodeString(response.Event.Element.Value)
			if err != nil {
				log.Errorf("Failed to decode list item: %v", err)
			} else {
				elem, err := s.codec.Decode(bytes)
				if err != nil {
					log.Error(err)
					continue
				}

				switch response.Event.Type {
				case api.Event_ADD:
					ch <- Event[E]{
						Type:  EventAdd,
						Value: elem,
					}
				case api.Event_REMOVE:
					ch <- Event[E]{
						Type:  EventRemove,
						Value: elem,
					}
				case api.Event_REPLAY:
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
