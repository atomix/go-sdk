// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
}

// NewClient creates a new primitive client
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

// Client is a base client for all primitives
type Client struct {
	primitiveType Type
	name          string
	client        primitiveapi.PrimitiveClient
	options       newOptions
}

// Type returns the primitive type
func (c *Client) Type() Type {
	return c.primitiveType
}

// SessionID returns the primitive session identifier
func (c *Client) SessionID() string {
	return c.options.sessionID
}

// Name returns the primitive name
func (c *Client) Name() string {
	return c.name
}

func (c *Client) getPrimitiveID() primitiveapi.PrimitiveId {
	return primitiveapi.PrimitiveId{
		Type: c.primitiveType.String(),
		Name: c.name,
	}
}

// GetHeaders gets the primitive headers
func (c *Client) GetHeaders() primitiveapi.RequestHeaders {
	return primitiveapi.RequestHeaders{
		PrimitiveID: c.getPrimitiveID(),
		ClusterKey:  c.options.clusterKey,
	}
}

// Create creates an instance of the primitive
func (c *Client) Create(ctx context.Context) error {
	request := &primitiveapi.CreateRequest{
		Headers: c.GetHeaders(),
	}
	_, err := c.client.Create(ctx, request)
	return errors.From(err)
}

// Close closes the primitive session
func (c *Client) Close(ctx context.Context) error {
	request := &primitiveapi.CloseRequest{
		Headers: c.GetHeaders(),
	}
	_, err := c.client.Close(ctx, request)
	return errors.From(err)
}
