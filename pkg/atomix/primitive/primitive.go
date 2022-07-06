// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
)

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
