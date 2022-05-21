// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
)

// Primitive is the base interface for primitives
type Primitive interface {
	// Name returns the primitive name
	Name() string

	// Type returns the primitive type name
	Type() string

	// Close closes the primitive
	Close(ctx context.Context) error
}

type Client interface {
	Connect(ctx context.Context) (*grpc.ClientConn, error)
}

type Provider[T Primitive, O any] interface {
	Get(ctx context.Context, name string, opts ...Option) func(opts ...O) (T, error)
}

func NewProvider[T Primitive, O any](provider func(ctx context.Context, name string, opts ...Option) func(...O) (T, error)) Provider[T, O] {
	return &primitiveProvider[T, O]{
		provider: provider,
	}
}

type primitiveProvider[T Primitive, O any] struct {
	provider func(ctx context.Context, name string, opts ...Option) func(...O) (T, error)
}

func (p *primitiveProvider[T, O]) Get(ctx context.Context, name string, opts ...Option) func(opts ...O) (T, error) {
	return p.provider(ctx, name, opts...)
}

func Open[C gogoproto.Message](client Client) func(ctx context.Context, primitiveType string, name string, config C, opts ...Option) (*ManagedPrimitive, *grpc.ClientConn, error) {
	return func(ctx context.Context, primitiveType string, name string, config C, opts ...Option) (*ManagedPrimitive, *grpc.ClientConn, error) {
		var options Options
		options.apply(opts...)

		configAny, err := types.MarshalAny(config)
		if err != nil {
			return nil, nil, err
		}

		primitive := &runtimev1.Primitive{
			ObjectMeta: runtimev1.ObjectMeta{
				ID: runtimev1.ObjectId{
					Name: name,
				},
				Labels: options.Labels,
			},
			Spec: runtimev1.PrimitiveSpec{
				Type:   primitiveType,
				Config: configAny,
			},
		}

		conn, err := client.Connect(ctx)
		if err != nil {
			return nil, nil, errors.FromProto(err)
		}

		manager := &ManagedPrimitive{
			client:    primitivev1.NewPrimitiveManagerClient(conn),
			primitive: primitive,
		}
		if err := manager.open(ctx); err != nil {
			return nil, nil, err
		}
		return manager, conn, nil
	}
}

type ManagedPrimitive struct {
	client    primitivev1.PrimitiveManagerClient
	primitive *runtimev1.Primitive
}

func (p *ManagedPrimitive) Name() string {
	return p.primitive.ID.Name
}

func (p *ManagedPrimitive) Type() string {
	return p.primitive.Spec.Type
}

func (p *ManagedPrimitive) GetHeaders() primitivev1.RequestHeaders {
	return primitivev1.RequestHeaders{
		PrimitiveID: p.primitive.ID,
	}
}

func (p *ManagedPrimitive) open(ctx context.Context) error {
	request := &primitivev1.OpenPrimitiveRequest{
		Primitive: *p.primitive,
	}
	_, err := p.client.Open(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (p *ManagedPrimitive) Close(ctx context.Context) error {
	request := &primitivev1.ClosePrimitiveRequest{
		PrimitiveID: p.primitive.ID,
	}
	_, err := p.client.Close(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

var _ Primitive = (*ManagedPrimitive)(nil)
