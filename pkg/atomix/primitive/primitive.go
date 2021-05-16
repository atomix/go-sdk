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
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"google.golang.org/grpc"
)

// Type is the type of a primitive
type Type string

func (t Type) String() string {
	return string(t)
}

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

func NewClient(primitiveType Type, name string, conn *grpc.ClientConn, opts ...Option) *Client {
	options := newOptions{}
	for _, opt := range opts {
		opt.applyNew(&options)
	}
	return &Client{
		primitiveType: primitiveType,
		name:          name,
		client:        primitiveapi.NewPrimitiveClient(conn),
		options:       options,
	}
}

type Client struct {
	primitiveType Type
	name          string
	client        primitiveapi.PrimitiveClient
	options       newOptions
}

func (c *Client) Type() Type {
	return c.primitiveType
}

func (c *Client) Name() string {
	return c.name
}

func (c *Client) getPrimitiveId() primitiveapi.PrimitiveId {
	return primitiveapi.PrimitiveId{
		Type: c.primitiveType.String(),
		Name: c.name,
	}
}

func (c *Client) GetHeaders() primitiveapi.RequestHeaders {
	return primitiveapi.RequestHeaders{
		PrimitiveID: c.getPrimitiveId(),
		ClusterKey:  c.options.cluster,
	}
}

func (c *Client) Create(ctx context.Context) error {
	request := &primitiveapi.CreateRequest{
		Headers: c.GetHeaders(),
	}
	_, err := c.client.Create(ctx, request)
	return errors.From(err)
}

func (c *Client) Close(ctx context.Context) error {
	request := &primitiveapi.CloseRequest{
		Headers: c.GetHeaders(),
	}
	_, err := c.client.Close(ctx, request)
	return errors.From(err)
}

func (c *Client) Delete(ctx context.Context) error {
	request := &primitiveapi.DeleteRequest{
		Headers: c.GetHeaders(),
	}
	_, err := c.client.Delete(ctx, request)
	return errors.From(err)
}
