// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	"encoding/base64"
	"github.com/atomix/atomix/api/errors"
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/types"
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
	// This is a non-blocking method. If the method returns without error, key/value paids will be pushed on to the
	// given channel and the channel will be closed once all elements have been read from the set.
	Elements(ctx context.Context) (ElementStream[E], error)

	// Watch watches the set for changes
	// This is a non-blocking method. If the method returns without error, set events will be pushed onto
	// the given channel in the order in which they occur.
	Watch(ctx context.Context) (ElementStream[E], error)

	// Events watches the set for change events
	// This is a non-blocking method. If the method returns without error, set events will be pushed onto
	// the given channel in the order in which they occur.
	Events(ctx context.Context) (EventStream[E], error)
}

type ElementStream[E any] stream.Stream[E]

type EventStream[E any] stream.Stream[Event[E]]

// Event is a set change event
type Event[E any] interface {
	event() *setv1.Event
}

type grpcEvent struct {
	proto *setv1.Event
}

func (e *grpcEvent) event() *setv1.Event {
	return e.proto
}

type Added[E any] struct {
	*grpcEvent
	Element E
}

type Removed[E any] struct {
	*grpcEvent
	Element E
	Expired bool
}

type setPrimitive[E any] struct {
	primitive.Primitive
	client setv1.SetClient
	codec  types.Codec[E]
}

func (s *setPrimitive[E]) Add(ctx context.Context, value E) (bool, error) {
	bytes, err := s.codec.Encode(value)
	if err != nil {
		return false, errors.NewInvalid("value encoding failed", err)
	}
	request := &setv1.AddRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.Name(),
		},
		Element: setv1.Element{
			Value: base64.StdEncoding.EncodeToString(bytes),
		},
	}
	_, err = s.client.Add(ctx, request)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *setPrimitive[E]) Remove(ctx context.Context, value E) (bool, error) {
	bytes, err := s.codec.Encode(value)
	if err != nil {
		return false, errors.NewInvalid("value encoding failed", err)
	}
	request := &setv1.RemoveRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.Name(),
		},
		Element: setv1.Element{
			Value: base64.StdEncoding.EncodeToString(bytes),
		},
	}
	_, err = s.client.Remove(ctx, request)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *setPrimitive[E]) Contains(ctx context.Context, value E) (bool, error) {
	bytes, err := s.codec.Encode(value)
	if err != nil {
		return false, errors.NewInvalid("value encoding failed", err)
	}
	request := &setv1.ContainsRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.Name(),
		},
		Element: setv1.Element{
			Value: base64.StdEncoding.EncodeToString(bytes),
		},
	}
	response, err := s.client.Contains(ctx, request)
	if err != nil {
		return false, err
	}
	return response.Contains, nil
}

func (s *setPrimitive[E]) Len(ctx context.Context) (int, error) {
	request := &setv1.SizeRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.Name(),
		},
	}
	response, err := s.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}
	return int(response.Size_), nil
}

func (s *setPrimitive[E]) Clear(ctx context.Context) error {
	request := &setv1.ClearRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.Name(),
		},
	}
	_, err := s.client.Clear(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (m *setPrimitive[E]) Elements(ctx context.Context) (ElementStream[E], error) {
	return m.elements(ctx, false)
}

func (m *setPrimitive[E]) Watch(ctx context.Context) (ElementStream[E], error) {
	return m.elements(ctx, true)
}

func (m *setPrimitive[E]) elements(ctx context.Context, watch bool) (ElementStream[E], error) {
	request := &setv1.ElementsRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Watch: watch,
	}
	client, err := m.client.Elements(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[E])
	go func() {
		defer close(ch)
		for {
			response, err := client.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				if errors.IsCanceled(err) || errors.IsTimeout(err) {
					return
				}
				log.Errorf("Entries failed: %v", err)
				return
			}
			bytes, err := base64.StdEncoding.DecodeString(response.Element.Value)
			if err != nil {
				log.Error(err)
				continue
			}
			element, err := m.codec.Decode(bytes)
			if err != nil {
				log.Error(err)
				continue
			}
			ch <- stream.Result[E]{
				Value: element,
			}
		}
	}()
	return stream.NewChannelStream[E](ch), nil
}

func (m *setPrimitive[E]) Events(ctx context.Context) (EventStream[E], error) {
	request := &setv1.EventsRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}

	client, err := m.client.Events(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[Event[E]])
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
			response, err := client.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
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

			switch e := response.Event.Event.(type) {
			case *setv1.Event_Added_:
				bytes, err := base64.StdEncoding.DecodeString(e.Added.Element.Value)
				if err != nil {
					log.Error(err)
					continue
				}
				element, err := m.codec.Decode(bytes)
				if err != nil {
					log.Error(err)
					continue
				}
				ch <- stream.Result[Event[E]]{
					Value: &Added[E]{
						grpcEvent: &grpcEvent{&response.Event},
						Element:   element,
					},
				}
			case *setv1.Event_Removed_:
				bytes, err := base64.StdEncoding.DecodeString(e.Removed.Element.Value)
				if err != nil {
					log.Error(err)
					continue
				}
				element, err := m.codec.Decode(bytes)
				if err != nil {
					log.Error(err)
					continue
				}
				ch <- stream.Result[Event[E]]{
					Value: &Removed[E]{
						grpcEvent: &grpcEvent{&response.Event},
						Element:   element,
						Expired:   e.Removed.Expired,
					},
				}
			}
		}
	}()

	select {
	case <-openCh:
		return stream.NewChannelStream[Event[E]](ch), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
