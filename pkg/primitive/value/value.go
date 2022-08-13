// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/go-client/pkg/generic"
	"github.com/atomix/go-client/pkg/primitive"
	"github.com/atomix/go-client/pkg/stream"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	valuev1 "github.com/atomix/runtime/api/atomix/runtime/value/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"io"
)

var log = logging.GetLogger()

// Value is a distributed value
type Value[V any] interface {
	primitive.Primitive

	// Set sets the value
	Set(ctx context.Context, value V) error

	// Get gets the value
	Get(ctx context.Context) (V, error)

	// Watch watches the map for changes
	// This is a non-blocking method. If the method returns without error, map events will be pushed onto
	// the given channel in the order in which they occur.
	Watch(ctx context.Context) (ValueStream[V], error)
}

type ValueStream[V any] stream.Stream[V]

type valuePrimitive[V any] struct {
	primitive.Primitive
	client valuev1.ValueClient
	codec  generic.Codec[V]
}

func (m *valuePrimitive[V]) Set(ctx context.Context, value V) error {
	bytes, err := m.codec.Encode(value)
	if err != nil {
		return errors.NewInvalid("value encoding failed", err)
	}
	request := &valuev1.SetRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Value: bytes,
	}
	_, err = m.client.Set(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (m *valuePrimitive[V]) Get(ctx context.Context) (V, error) {
	request := &valuev1.GetRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	var value V
	response, err := m.client.Get(ctx, request)
	if err != nil {
		return value, errors.FromProto(err)
	}
	value, err = m.codec.Decode(response.Value)
	if err != nil {
		return value, errors.NewInvalid("value decoding failed", err)
	}
	return value, nil
}

func (m *valuePrimitive[V]) Watch(ctx context.Context) (ValueStream[V], error) {
	request := &valuev1.WatchRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	client, err := m.client.Watch(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}

	ch := make(chan stream.Result[V])
	go func() {
		defer close(ch)
		for {
			response, err := client.Recv()
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
			value, err := m.codec.Decode(response.Value)
			if err != nil {
				log.Error(err)
			} else {
				ch <- stream.Result[V]{
					Value: value,
				}
			}
		}
	}()
	return stream.NewChannelStream[V](ch), nil
}

func (m *valuePrimitive[V]) create(ctx context.Context, tags map[string]string) error {
	request := &valuev1.CreateRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
		Tags: tags,
	}
	_, err := m.client.Create(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (m *valuePrimitive[V]) Close(ctx context.Context) error {
	request := &valuev1.CloseRequest{
		ID: runtimev1.PrimitiveId{
			Name: m.Name(),
		},
	}
	_, err := m.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
