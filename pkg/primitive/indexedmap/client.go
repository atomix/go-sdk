// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	indexedmapv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"io"
)

func newIndexedMapsClient(name string, client indexedmapv1.IndexedMapsClient) primitive.Primitive {
	return &indexedMapsClient{
		name:   name,
		client: client,
	}
}

type indexedMapsClient struct {
	name   string
	client indexedmapv1.IndexedMapsClient
}

func (s *indexedMapsClient) Name() string {
	return s.name
}

func (s *indexedMapsClient) Close(ctx context.Context) error {
	_, err := s.client.Close(ctx, &indexedmapv1.CloseRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.name,
		},
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func newIndexedMapClient(name string, client indexedmapv1.IndexedMapClient) IndexedMap[string, []byte] {
	return &indexedMapClient{
		Primitive: newIndexedMapsClient(name, client),
		client:    client,
	}
}

// indexedMapClient is the default single-partition implementation of Map
type indexedMapClient struct {
	primitive.Primitive
	client indexedmapv1.IndexedMapClient
}

func (m *indexedMapClient) Append(ctx context.Context, key string, value []byte, opts ...AppendOption) (*Entry[string, []byte], error) {
	request := &indexedmapv1.AppendRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Key:   key,
		Value: value,
	}
	for i := range opts {
		opts[i].beforeAppend(request)
	}
	response, err := m.client.Append(ctx, request)
	if err != nil {
		return nil, err
	}
	for i := range opts {
		opts[i].afterAppend(response)
	}
	return &Entry[string, []byte]{
		Key:   key,
		Index: Index(response.Entry.Index),
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Entry.Value.Value,
			Version: primitive.Version(response.Entry.Value.Version),
		},
	}, nil
}

func (m *indexedMapClient) Update(ctx context.Context, key string, value []byte, opts ...UpdateOption) (*Entry[string, []byte], error) {
	request := &indexedmapv1.UpdateRequest{
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
		Key:   key,
		Index: Index(response.Entry.Index),
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Entry.Value.Value,
			Version: primitive.Version(response.Entry.Value.Version),
		},
	}, nil
}

func (m *indexedMapClient) Get(ctx context.Context, key string, opts ...GetOption) (*Entry[string, []byte], error) {
	request := &indexedmapv1.GetRequest{
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
		Key:   key,
		Index: Index(response.Entry.Index),
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Entry.Value.Value,
			Version: primitive.Version(response.Entry.Value.Version),
		},
	}, nil
}

func (m *indexedMapClient) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry[string, []byte], error) {
	request := &indexedmapv1.GetRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
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
		Key:   response.Entry.Key,
		Index: Index(response.Entry.Index),
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Entry.Value.Value,
			Version: primitive.Version(response.Entry.Value.Version),
		},
	}, nil
}

func (m *indexedMapClient) FirstIndex(ctx context.Context) (Index, error) {
	request := &indexedmapv1.FirstEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	response, err := m.client.FirstEntry(ctx, request)
	if err != nil {
		return 0, err
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapClient) LastIndex(ctx context.Context) (Index, error) {
	request := &indexedmapv1.LastEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	response, err := m.client.LastEntry(ctx, request)
	if err != nil {
		return 0, err
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapClient) PrevIndex(ctx context.Context, index Index) (Index, error) {
	request := &indexedmapv1.PrevEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.PrevEntry(ctx, request)
	if err != nil {
		return 0, err
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapClient) NextIndex(ctx context.Context, index Index) (Index, error) {
	request := &indexedmapv1.NextEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.NextEntry(ctx, request)
	if err != nil {
		return 0, err
	}
	return Index(response.Entry.Index), nil
}

func (m *indexedMapClient) FirstEntry(ctx context.Context) (*Entry[string, []byte], error) {
	request := &indexedmapv1.FirstEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	response, err := m.client.FirstEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return &Entry[string, []byte]{
		Key:   response.Entry.Key,
		Index: Index(response.Entry.Index),
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Entry.Value.Value,
			Version: primitive.Version(response.Entry.Value.Version),
		},
	}, nil
}

func (m *indexedMapClient) LastEntry(ctx context.Context) (*Entry[string, []byte], error) {
	request := &indexedmapv1.LastEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
	}
	response, err := m.client.LastEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return &Entry[string, []byte]{
		Key:   response.Entry.Key,
		Index: Index(response.Entry.Index),
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Entry.Value.Value,
			Version: primitive.Version(response.Entry.Value.Version),
		},
	}, nil
}

func (m *indexedMapClient) PrevEntry(ctx context.Context, index Index) (*Entry[string, []byte], error) {
	request := &indexedmapv1.PrevEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.PrevEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return &Entry[string, []byte]{
		Key:   response.Entry.Key,
		Index: Index(response.Entry.Index),
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Entry.Value.Value,
			Version: primitive.Version(response.Entry.Value.Version),
		},
	}, nil
}

func (m *indexedMapClient) NextEntry(ctx context.Context, index Index) (*Entry[string, []byte], error) {
	request := &indexedmapv1.NextEntryRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
	}
	response, err := m.client.NextEntry(ctx, request)
	if err != nil {
		return nil, err
	}
	return &Entry[string, []byte]{
		Key:   response.Entry.Key,
		Index: Index(response.Entry.Index),
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Entry.Value.Value,
			Version: primitive.Version(response.Entry.Value.Version),
		},
	}, nil
}

func (m *indexedMapClient) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry[string, []byte], error) {
	request := &indexedmapv1.RemoveRequest{
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
		Key:   response.Entry.Key,
		Index: Index(response.Entry.Index),
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Entry.Value.Value,
			Version: primitive.Version(response.Entry.Value.Version),
		},
	}, nil
}

func (m *indexedMapClient) RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry[string, []byte], error) {
	request := &indexedmapv1.RemoveRequest{
		ID: runtimev1.PrimitiveID{
			Name: m.Name(),
		},
		Index: uint64(index),
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
		Key:   response.Entry.Key,
		Index: Index(response.Entry.Index),
		Versioned: primitive.Versioned[[]byte]{
			Value:   response.Entry.Value.Value,
			Version: primitive.Version(response.Entry.Value.Version),
		},
	}, nil
}

func (m *indexedMapClient) Len(ctx context.Context) (int, error) {
	request := &indexedmapv1.SizeRequest{
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

func (m *indexedMapClient) Clear(ctx context.Context) error {
	request := &indexedmapv1.ClearRequest{
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

func (m *indexedMapClient) List(ctx context.Context) (EntryStream[string, []byte], error) {
	return m.entries(ctx, false)
}

func (m *indexedMapClient) Watch(ctx context.Context) (EntryStream[string, []byte], error) {
	return m.entries(ctx, true)
}

func (m *indexedMapClient) entries(ctx context.Context, watch bool) (EntryStream[string, []byte], error) {
	request := &indexedmapv1.EntriesRequest{
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
					Key:   response.Entry.Key,
					Index: Index(response.Entry.Index),
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

func (m *indexedMapClient) Events(ctx context.Context, opts ...EventsOption) (EventStream[string, []byte], error) {
	request := &indexedmapv1.EventsRequest{
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
			case *indexedmapv1.Event_Inserted_:
				ch <- stream.Result[Event[string, []byte]]{
					Value: &Inserted[string, []byte]{
						grpcEvent: &grpcEvent{&response.Event},
						Entry: &Entry[string, []byte]{
							Key:   response.Event.Key,
							Index: Index(response.Event.Index),
							Versioned: primitive.Versioned[[]byte]{
								Value:   e.Inserted.Value.Value,
								Version: primitive.Version(e.Inserted.Value.Version),
							},
						},
					},
				}
			case *indexedmapv1.Event_Updated_:
				ch <- stream.Result[Event[string, []byte]]{
					Value: &Updated[string, []byte]{
						grpcEvent: &grpcEvent{&response.Event},
						Entry: &Entry[string, []byte]{
							Key:   response.Event.Key,
							Index: Index(response.Event.Index),
							Versioned: primitive.Versioned[[]byte]{
								Value:   e.Updated.Value.Value,
								Version: primitive.Version(e.Updated.Value.Version),
							},
						},
						PrevEntry: &Entry[string, []byte]{
							Key:   response.Event.Key,
							Index: Index(response.Event.Index),
							Versioned: primitive.Versioned[[]byte]{
								Value:   e.Updated.PrevValue.Value,
								Version: primitive.Version(e.Updated.PrevValue.Version),
							},
						},
					},
				}
			case *indexedmapv1.Event_Removed_:
				ch <- stream.Result[Event[string, []byte]]{
					Value: &Removed[string, []byte]{
						grpcEvent: &grpcEvent{&response.Event},
						Entry: &Entry[string, []byte]{
							Key:   response.Event.Key,
							Index: Index(response.Event.Index),
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
