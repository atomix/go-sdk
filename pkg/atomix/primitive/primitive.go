// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc/metadata"
)

type ID struct {
	Application string
	Primitive   string
	Session     string
}

func AppendToOutgoingContext(ctx context.Context, id ID) context.Context {
	return metadata.AppendToOutgoingContext(ctx,
		primitive.ApplicationIDHeader, id.Application,
		primitive.PrimitiveIDHeader, id.Primitive,
		primitive.SessionIDHeader, id.Session)
}

// Primitive is the base interface for primitives
type Primitive interface {
	// ID returns the primitive ID
	ID() ID

	// Close closes the primitive
	Close(ctx context.Context) error
}

func New(id ID) Primitive {
	return &managedPrimitive{
		id: id,
	}
}

type managedPrimitive struct {
	Primitive
	id ID
}

func (p *managedPrimitive) ID() ID {
	return p.id
}
