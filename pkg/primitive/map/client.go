// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"io"
)

func newMapsClient(name string, client mapv1.MapsClient) primitive.Primitive {
	return &mapsClient{
		name:   name,
		client: client,
	}
}

type mapsClient struct {
	name   string
	client mapv1.MapsClient
}

func (s *mapsClient) Name() string {
	return s.name
}

func (s *mapsClient) Close(ctx context.Context) error {
	_, err := s.client.Close(ctx, &mapv1.CloseRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.name,
		},
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func newMapClient(name string, client mapv1.MapClient) Map[string, []byte] {
	return &mapClient{
		Primitive: newMapsClient(name, client),
		client:    client,
	}
}

type mapClient struct {
	primitive.Primitive
	client mapv1.MapClient
}

func (m *mapClient) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry[string, []byte], error) {
	request := &mapv1.PutRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Key:   key,
		Value: value,
	}
	for i := range opts {
		opts[i].beforePut(request)
	}
	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterPut(response)
	}
	return &Entry[string, []byte]{
		Versioned: primitive.Versioned[[]byte]{
			Value:   value,
			Version: primitive.Version(response.Version),
		},
		Key: key,
	}, nil
}

func (m *mapClient) Insert(ctx context.Context, key string, value []byte, opts ...InsertOption) (*Entry[string, []byte], error) {
	request := &mapv1.InsertRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Key:   key,
		Value: value,
	}
	for i := range opts {
		opts[i].beforeInsert(request)
	}
	response, err := m.client.Insert(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterInsert(response)
	}
	return &Entry[string, []byte]{
		Versioned: primitive.Versioned[[]byte]{
			Value:   value,
			Version: primitive.Version(response.Version),
		},
		Key: key,
	}, nil
}

func (m *mapClient) Update(ctx context.Context, key string, value []byte, opts ...UpdateOption) (*Entry[string, []byte], error) {
	request := &mapv1.UpdateRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Key:   key,
		Value: value,
	}
	for i := range opts {
		opts[i].beforeUpdate(request)
	}
	response, err := m.client.Update(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterUpdate(response)
	}
	return &Entry[string, []byte]{
		Versioned: primitive.Versioned[[]byte]{
			Value:   value,
			Version: primitive.Version(response.Version),
		},
		Key: key,
	}, nil
}

func (m *mapClient) Get(ctx context.Context, key string, opts ...GetOption) (*Entry[string, []byte], error) {
	request := &mapv1.GetRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Key: key,
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	return &Entry[string, []byte]{
		Key: key,
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Value.Value,
			Version: primitive.Version(response.Value.Version),
		},
	}, nil
}

func (m *mapClient) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry[string, []byte], error) {
	request := &mapv1.RemoveRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Key: key,
	}
	for i := range opts {
		opts[i].beforeRemove(request)
	}
	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterRemove(response)
	}
	return &Entry[string, []byte]{
		Key: key,
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Value.Value,
			Version: primitive.Version(response.Value.Version),
		},
	}, nil
}

func (m *mapClient) Len(ctx context.Context) (int, error) {
	request := &mapv1.SizeRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}
	return int(response.Size_), nil
}

func (m *mapClient) Clear(ctx context.Context) error {
	request := &mapv1.ClearRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	_, err := m.client.Clear(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (m *mapClient) List(ctx context.Context) (EntryStream[string, []byte], error) {
	return m.entries(ctx, false)
}

func (m *mapClient) Watch(ctx context.Context) (EntryStream[string, []byte], error) {
	return m.entries(ctx, true)
}

func (m *mapClient) entries(ctx context.Context, watch bool) (EntryStream[string, []byte], error) {
	request := &mapv1.EntriesRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Watch: watch,
	}
	client, err := m.client.Entries(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[*Entry[string, []byte]])
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
			ch <- stream.Result[*Entry[string, []byte]]{
				Value: &Entry[string, []byte]{
					Key: response.Entry.Key,
					Versioned: primitive.Versioned[[]byte]{
						Value:   response.Entry.Value.Value,
						Version: primitive.Version(response.Entry.Value.Version),
					},
				},
			}
		}
	}()
	return stream.NewChannelStream[*Entry[string, []byte]](ch), nil
}

func (m *mapClient) Events(ctx context.Context, opts ...EventsOption) (EventStream[string, []byte], error) {
	request := &mapv1.EventsRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeEvents(request)
	}

	client, err := m.client.Events(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[Event[string, []byte]])
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
			case *mapv1.Event_Inserted_:
				ch <- stream.Result[Event[string, []byte]]{
					Value: &Inserted[string, []byte]{
						grpcEvent: &grpcEvent{&response.Event},
						Entry: &Entry[string, []byte]{
							Key: response.Event.Key,
							Versioned: primitive.Versioned[[]byte]{
								Value:   e.Inserted.Value.Value,
								Version: primitive.Version(e.Inserted.Value.Version),
							},
						},
					},
				}
			case *mapv1.Event_Updated_:
				ch <- stream.Result[Event[string, []byte]]{
					Value: &Updated[string, []byte]{
						grpcEvent: &grpcEvent{&response.Event},
						PrevEntry: &Entry[string, []byte]{
							Key: response.Event.Key,
							Versioned: primitive.Versioned[[]byte]{
								Value:   e.Updated.PrevValue.Value,
								Version: primitive.Version(e.Updated.PrevValue.Version),
							},
						},
						Entry: &Entry[string, []byte]{
							Key: response.Event.Key,
							Versioned: primitive.Versioned[[]byte]{
								Value:   e.Updated.Value.Value,
								Version: primitive.Version(e.Updated.Value.Version),
							},
						},
					},
				}
			case *mapv1.Event_Removed_:
				ch <- stream.Result[Event[string, []byte]]{
					Value: &Removed[string, []byte]{
						grpcEvent: &grpcEvent{&response.Event},
						Entry: &Entry[string, []byte]{
							Key: response.Event.Key,
							Versioned: primitive.Versioned[[]byte]{
								Value:   e.Removed.Value.Value,
								Version: primitive.Version(e.Removed.Value.Version),
							},
						},
						Expired: e.Removed.Expired,
					},
				}
			}
		}
	}()

	select {
	case <-openCh:
		return stream.NewChannelStream[Event[string, []byte]](ch), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
