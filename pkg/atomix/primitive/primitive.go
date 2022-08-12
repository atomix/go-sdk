// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"google.golang.org/grpc"
)

type Builder[B Builder[B, T], T Primitive] interface {
	Tag(key, value string) B
	Tags(tags map[string]string) B
	Get(ctx context.Context) (T, error)
}

func NewOptions(name string) *Options {
	return &Options{
		Name: name,
		Tags: make(map[string]string),
	}
}

type Options struct {
	Name string
	Tags map[string]string
}

func (o *Options) SetTag(key, value string) {
	if o.Tags == nil {
		o.Tags = make(map[string]string)
	}
	o.Tags[key] = value
}

func (o *Options) SetTags(tags map[string]string) {
	o.Tags = tags
}

type Client interface {
	Connect(context.Context) (*grpc.ClientConn, error)
}

// Primitive is the base interface for primitives
type Primitive interface {
	// Name returns the primitive ID
	Name() string

	// Close closes the primitive
	Close(ctx context.Context) error
}

func New(name string) Primitive {
	return &managedPrimitive{
		name: name,
	}
}

type managedPrimitive struct {
	Primitive
	name string
}

func (p *managedPrimitive) Name() string {
	return p.name
}
