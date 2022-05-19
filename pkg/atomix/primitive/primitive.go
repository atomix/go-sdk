// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
)

// Primitive is the base interface for primitives
type Primitive interface {
	// Name returns the primitive name
	Name() string

	// Close closes the primitive
	Close(ctx context.Context) error
}

type Client[T Primitive, O any] interface {
	Get(ctx context.Context, name string, opts ...Option) func(opts ...O) (T, error)
}

func NewClient[T Primitive, O any](manager Manager, getter func(primitive *ManagedPrimitive, opts ...O) (T, error)) Client[T, O] {
	return &primitiveClient[T, O]{
		manager: manager,
		getter:  getter,
	}
}

type primitiveClient[T Primitive, O any] struct {
	manager Manager
	getter  func(*ManagedPrimitive, ...O) (T, error)
}

func (c *primitiveClient[T, O]) Get(ctx context.Context, name string, primitiveOpts ...Option) func(...O) (T, error) {
	return func(clientOpts ...O) (T, error) {
		var options Options
		options.apply(primitiveOpts...)
		primitive := runtimev1.Primitive{
			PrimitiveMeta: runtimev1.PrimitiveMeta{
				PrimitiveID: runtimev1.PrimitiveId{
					Name: name,
				},
				Labels: options.Labels,
			},
		}
		if err := c.manager.Create(ctx, primitive); err != nil {
			return nil, err
		}
		return c.getter(newManagedPrimitive(c.manager, name), clientOpts...)
	}
}

type Manager interface {
	Create(ctx context.Context, primitive runtimev1.Primitive) error
	Close(ctx context.Context, primitiveID runtimev1.PrimitiveId) error
}

func newManagedPrimitive(manager Manager, name string) *ManagedPrimitive {
	return &ManagedPrimitive{
		manager: manager,
		name:    name,
	}
}

type ManagedPrimitive struct {
	manager Manager
	name    string
}

func (p *ManagedPrimitive) Name() string {
	return p.name
}

func (p *ManagedPrimitive) GetHeaders() runtimev1.RequestHeaders {
	return runtimev1.RequestHeaders{
		PrimitiveID: runtimev1.PrimitiveId{
			Name: p.name,
		},
	}
}

func (p *ManagedPrimitive) Close(ctx context.Context) error {
	primitiveID := runtimev1.PrimitiveId{
		Name: p.name,
	}
	return p.manager.Close(ctx, primitiveID)
}

var _ Primitive = (*ManagedPrimitive)(nil)
