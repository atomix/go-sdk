// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package primitive

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/headers"
	"google.golang.org/grpc"
)

// Type is the type of a primitive
type Type string

// Primitive is the base interface for primitives
type Primitive interface {
	// Type returns the primitive type
	Type() Type

	// Name returns the primitive name
	Name() string

	// Close closes the primitive
	Close(ctx context.Context) error

	// Delete deletes the primitive state from the cluster
	Delete(ctx context.Context) error
}

func NewClient(t Type, n string, conn *grpc.ClientConn) *Client {
	return &Client{
		primitiveType: t,
		name:          n,
		client:        primitiveapi.NewPrimitiveServiceClient(conn),
	}
}

type Client struct {
	primitiveType Type
	name          string
	client        primitiveapi.PrimitiveServiceClient
}

func (c *Client) Type() Type {
	return c.primitiveType
}

func (c *Client) Name() string {
	return c.name
}

func (c *Client) AddHeaders(ctx context.Context) context.Context {
	ctx = headers.PrimitiveType.SetString(ctx, string(c.primitiveType))
	ctx = headers.PrimitiveName.SetString(ctx, c.name)
	return ctx
}

func (c *Client) Create(ctx context.Context) error {
	request := &primitiveapi.CreateRequest{
		Type: string(c.Type()),
		Name: c.name,
	}
	_, err := c.client.Create(c.AddHeaders(ctx), request)
	return errors.From(err)
}

func (c *Client) Close(ctx context.Context) error {
	request := &primitiveapi.CloseRequest{
		Type: string(c.Type()),
		Name: c.name,
	}
	_, err := c.client.Close(c.AddHeaders(ctx), request)
	return errors.From(err)
}

func (c *Client) Delete(ctx context.Context) error {
	request := &primitiveapi.DeleteRequest{
		Type: string(c.Type()),
		Name: c.name,
	}
	_, err := c.client.Delete(c.AddHeaders(ctx), request)
	return errors.From(err)
}
