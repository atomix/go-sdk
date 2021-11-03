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
	"fmt"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive/v1"
	"google.golang.org/grpc/metadata"
)

const sessionIDKey = "session-id"

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
func NewClient(primitiveType Type, name string, session primitiveapi.SessionID) *Client {
	return &Client{
		primitiveType: primitiveType,
		name:          name,
		session:       session,
	}
}

// Client is a base client for all primitives
type Client struct {
	primitiveType Type
	name          string
	session       primitiveapi.SessionID
}

// Type returns the primitive type
func (c *Client) Type() Type {
	return c.primitiveType
}

// SessionID returns the primitive session identifier
func (c *Client) SessionID() primitiveapi.SessionID {
	return c.session
}

// Name returns the primitive name
func (c *Client) Name() string {
	return c.name
}

// GetContext returns the primitive context
func (c *Client) GetContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, sessionIDKey, fmt.Sprint(c.session))
}
