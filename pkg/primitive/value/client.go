// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	valuev1 "github.com/atomix/atomix/api/runtime/value/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"io"
)

func newValuesClient(name string, client valuev1.ValuesClient) primitive.Primitive {
	return &valuesClient{
		name:   name,
		client: client,
	}
}

type valuesClient struct {
	name   string
	client valuev1.ValuesClient
}

func (s *valuesClient) Name() string {
	return s.name
}

func (s *valuesClient) Close(ctx context.Context) error {
	_, err := s.client.Close(ctx, &valuev1.CloseRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.name,
		},
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func newValueClient(name string, client valuev1.ValueClient) Value[[]byte] {
	return &valueClient{
		Primitive: newValuesClient(name, client),
		client:    client,
	}
}

type valueClient struct {
	primitive.Primitive
	client valuev1.ValueClient
}

func (v *valueClient) Set(ctx context.Context, value []byte, opts ...SetOption) (primitive.Versioned[[]byte], error) {
	request := &valuev1.SetRequest{
		ID: runtimev1.PrimitiveID{
			Name: v.Name(),
		},
		Value: value,
	}
	for i := range opts {
		opts[i].beforeSet(request)
	}
	response, err := v.client.Set(ctx, request)
	if err != nil {
		return primitive.Versioned[[]byte]{}, err
	}
	for i := range opts {
		opts[i].afterSet(response)
	}
	return primitive.Versioned[[]byte]{
		Version: primitive.Version(response.Version),
		Value:   value,
	}, nil
}

func (v *valueClient) Update(ctx context.Context, value []byte, opts ...UpdateOption) (primitive.Versioned[[]byte], error) {
	request := &valuev1.UpdateRequest{
		ID: runtimev1.PrimitiveID{
			Name: v.Name(),
		},
		Value: value,
	}
	for i := range opts {
		opts[i].beforeUpdate(request)
	}
	response, err := v.client.Update(ctx, request)
	if err != nil {
		return primitive.Versioned[[]byte]{}, err
	}
	for i := range opts {
		opts[i].afterUpdate(response)
	}
	return primitive.Versioned[[]byte]{
		Version: primitive.Version(response.Version),
		Value:   value,
	}, nil
}

func (v *valueClient) Get(ctx context.Context, opts ...GetOption) (primitive.Versioned[[]byte], error) {
	request := &valuev1.GetRequest{
		ID: runtimev1.PrimitiveID{
			Name: v.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := v.client.Get(ctx, request)
	if err != nil {
		return primitive.Versioned[[]byte]{}, err
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	return primitive.Versioned[[]byte]{
		Version: primitive.Version(response.Value.Version),
		Value:   response.Value.Value,
	}, nil
}

func (v *valueClient) Delete(ctx context.Context, opts ...DeleteOption) error {
	request := &valuev1.DeleteRequest{
		ID: runtimev1.PrimitiveID{
			Name: v.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeDelete(request)
	}
	response, err := v.client.Delete(ctx, request)
	if err != nil {
		return err
	}
	for i := range opts {
		opts[i].afterDelete(response)
	}
	return nil
}

func (v *valueClient) Watch(ctx context.Context) (ValueStream[[]byte], error) {
	request := &valuev1.WatchRequest{
		ID: runtimev1.PrimitiveID{
			Name: v.Name(),
		},
	}
	client, err := v.client.Watch(ctx, request)
	if err != nil {
		return nil, err
	}

	ch := make(chan stream.Result[primitive.Versioned[[]byte]])
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
				log.Errorf("Watch failed: %v", err)
				return
			}
			ch <- stream.Result[primitive.Versioned[[]byte]]{
				Value: primitive.Versioned[[]byte]{
					Version: primitive.Version(response.Value.Version),
					Value:   response.Value.Value,
				},
			}
		}
	}()
	return stream.NewChannelStream[primitive.Versioned[[]byte]](ch), nil
}

func (v *valueClient) Events(ctx context.Context, opts ...EventsOption) (EventStream[[]byte], error) {
	request := &valuev1.EventsRequest{
		ID: runtimev1.PrimitiveID{
			Name: v.Name(),
		},
	}
	for i := range opts {
		opts[i].beforeEvents(request)
	}

	client, err := v.client.Events(ctx, request)
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
			case *valuev1.Event_Created_:
				ch <- stream.Result[Event[[]byte]]{
					Value: &Created[[]byte]{
						grpcEvent: &grpcEvent{&response.Event},
						Value: primitive.Versioned[[]byte]{
							Version: primitive.Version(e.Created.Value.Version),
							Value:   e.Created.Value.Value,
						},
					},
				}
			case *valuev1.Event_Updated_:
				ch <- stream.Result[Event[[]byte]]{
					Value: &Updated[[]byte]{
						grpcEvent: &grpcEvent{&response.Event},
						Value: primitive.Versioned[[]byte]{
							Version: primitive.Version(e.Updated.Value.Version),
							Value:   e.Updated.Value.Value,
						},
						PrevValue: primitive.Versioned[[]byte]{
							Version: primitive.Version(e.Updated.PrevValue.Version),
							Value:   e.Updated.PrevValue.Value,
						},
					},
				}
			case *valuev1.Event_Deleted_:
				ch <- stream.Result[Event[[]byte]]{
					Value: &Deleted[[]byte]{
						grpcEvent: &grpcEvent{&response.Event},
						Value: primitive.Versioned[[]byte]{
							Version: primitive.Version(e.Deleted.Value.Version),
							Value:   e.Deleted.Value.Value,
						},
						Expired: e.Deleted.Expired,
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
