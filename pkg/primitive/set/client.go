// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"io"
)

func newSetsClient(name string, client setv1.SetsClient) primitive.Primitive {
	return &setsClient{
		name:   name,
		client: client,
	}
}

type setsClient struct {
	name   string
	client setv1.SetsClient
}

func (s *setsClient) Name() string {
	return s.name
}

func (s *setsClient) Close(ctx context.Context) error {
	_, err := s.client.Close(ctx, &setv1.CloseRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.name,
		},
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func newSetClient(name string, client setv1.SetClient) Set[string] {
	return &setClient{
		Primitive: newSetsClient(name, client),
		client:    client,
	}
}

type setClient struct {
	primitive.Primitive
	client setv1.SetClient
}

func (s *setClient) Add(ctx context.Context, value string) (bool, error) {
	request := &setv1.AddRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.Name(),
		},
		Element: setv1.Element{
			Value: value,
		},
	}
	_, err := s.client.Add(ctx, request)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *setClient) Remove(ctx context.Context, value string) (bool, error) {
	request := &setv1.RemoveRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.Name(),
		},
		Element: setv1.Element{
			Value: value,
		},
	}
	_, err := s.client.Remove(ctx, request)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *setClient) Contains(ctx context.Context, value string) (bool, error) {
	request := &setv1.ContainsRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.Name(),
		},
		Element: setv1.Element{
			Value: value,
		},
	}
	response, err := s.client.Contains(ctx, request)
	if err != nil {
		return false, err
	}
	return response.Contains, nil
}

func (s *setClient) Len(ctx context.Context) (int, error) {
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

func (s *setClient) Clear(ctx context.Context) error {
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

func (s *setClient) Elements(ctx context.Context) (ElementStream[string], error) {
	return s.elements(ctx, false)
}

func (s *setClient) Watch(ctx context.Context) (ElementStream[string], error) {
	return s.elements(ctx, true)
}

func (s *setClient) elements(ctx context.Context, watch bool) (ElementStream[string], error) {
	request := &setv1.ElementsRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.Name(),
		},
		Watch: watch,
	}
	client, err := s.client.Elements(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[string])
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
			ch <- stream.Result[string]{
				Value: response.Element.Value,
			}
		}
	}()
	return stream.NewChannelStream[string](ch), nil
}

func (s *setClient) Events(ctx context.Context) (EventStream[string], error) {
	request := &setv1.EventsRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.Name(),
		},
	}

	client, err := s.client.Events(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[Event[string]])
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
				ch <- stream.Result[Event[string]]{
					Value: &Added[string]{
						grpcEvent: &grpcEvent{&response.Event},
						Element:   e.Added.Element.Value,
					},
				}
			case *setv1.Event_Removed_:
				ch <- stream.Result[Event[string]]{
					Value: &Removed[string]{
						grpcEvent: &grpcEvent{&response.Event},
						Element:   e.Removed.Element.Value,
						Expired:   e.Removed.Expired,
					},
				}
			}
		}
	}()

	select {
	case <-openCh:
		return stream.NewChannelStream[Event[string]](ch), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
