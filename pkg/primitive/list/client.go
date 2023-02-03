// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	listv1 "github.com/atomix/atomix/api/runtime/list/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"io"
)

func newListsClient(name string, client listv1.ListsClient) primitive.Primitive {
	return &listsClient{
		name:   name,
		client: client,
	}
}

type listsClient struct {
	name   string
	client listv1.ListsClient
}

func (s *listsClient) Name() string {
	return s.name
}

func (s *listsClient) Close(ctx context.Context) error {
	_, err := s.client.Close(ctx, &listv1.CloseRequest{})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func newListClient(name string, client listv1.ListClient) List[[]byte] {
	return &listClient{
		Primitive: newListsClient(name, client),
		client:    client,
	}
}

// listClient is the single partition implementation of List
type listClient struct {
	primitive.Primitive
	client listv1.ListClient
}

func (l *listClient) Append(ctx context.Context, value []byte) error {
	request := &listv1.AppendRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Value: listv1.Value{
			Value: value,
		},
	}
	_, err := l.client.Append(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (l *listClient) Insert(ctx context.Context, index int, value []byte) error {
	request := &listv1.InsertRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Index: uint32(index),
		Value: listv1.Value{
			Value: value,
		},
	}
	_, err := l.client.Insert(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (l *listClient) Set(ctx context.Context, index int, value []byte) error {
	request := &listv1.SetRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Index: uint32(index),
		Value: listv1.Value{
			Value: value,
		},
	}
	_, err := l.client.Set(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (l *listClient) Get(ctx context.Context, index int) ([]byte, error) {
	request := &listv1.GetRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Index: uint32(index),
	}
	response, err := l.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.Item.Value.Value, nil
}

func (l *listClient) Remove(ctx context.Context, index int) ([]byte, error) {
	request := &listv1.RemoveRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Index: uint32(index),
	}
	response, err := l.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.Item.Value.Value, nil
}

func (l *listClient) Len(ctx context.Context) (int, error) {
	request := &listv1.SizeRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
	}
	response, err := l.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}
	return int(response.Size_), nil
}

func (l *listClient) Items(ctx context.Context) (ItemStream[[]byte], error) {
	return l.items(ctx, false)
}

func (l *listClient) Watch(ctx context.Context) (ItemStream[[]byte], error) {
	return l.items(ctx, true)
}

func (l *listClient) items(ctx context.Context, watch bool) (ItemStream[[]byte], error) {
	request := &listv1.ItemsRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
		Watch: watch,
	}
	client, err := l.client.Items(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[[]byte])
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
			ch <- stream.Result[[]byte]{
				Value: response.Item.Value.Value,
			}
		}
	}()
	return stream.NewChannelStream[[]byte](ch), nil
}

func (l *listClient) Events(ctx context.Context, opts ...EventsOption) (EventStream[[]byte], error) {
	request := &listv1.EventsRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeEvents(request)
	}

	client, err := l.client.Events(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[Event[[]byte]])
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

			for i := range opts {
				opts[i].afterEvents(response)
			}

			switch e := response.Event.Event.(type) {
			case *listv1.Event_Inserted_:
				ch <- stream.Result[Event[[]byte]]{
					Value: &Inserted[[]byte]{
						grpcEvent: &grpcEvent{&response.Event},
						Value:     e.Inserted.Value.Value,
					},
				}
			case *listv1.Event_Updated_:
				ch <- stream.Result[Event[[]byte]]{
					Value: &Updated[[]byte]{
						grpcEvent: &grpcEvent{&response.Event},
						NewValue:  e.Updated.Value.Value,
						OldValue:  e.Updated.PrevValue.Value,
					},
				}
			case *listv1.Event_Removed_:
				ch <- stream.Result[Event[[]byte]]{
					Value: &Removed[[]byte]{
						grpcEvent: &grpcEvent{&response.Event},
						Value:     e.Removed.Value.Value,
					},
				}
			}
		}
	}()

	select {
	case <-openCh:
		return stream.NewChannelStream[Event[[]byte]](ch), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *listClient) Clear(ctx context.Context) error {
	request := &listv1.ClearRequest{
		ID: runtimev1.PrimitiveID{
			Name: l.Name(),
		},
	}
	_, err := l.client.Clear(ctx, request)
	if err != nil {
		return err
	}
	return nil
}
