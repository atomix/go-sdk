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

package counter

import (
	"context"
	api "github.com/atomix/atomix-api/go/atomix/primitive/counter/v1"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"google.golang.org/grpc"
)

// Type is the counter type
const Type primitive.Type = "Counter"

// Client provides an API for creating Counters
type Client interface {
	// GetCounter gets the Counter instance of the given name
	GetCounter(ctx context.Context, name string, opts ...Option) (Counter, error)
}

// Counter provides a distributed atomic counter
type Counter interface {
	primitive.Primitive

	// Get gets the current value of the counter
	Get(ctx context.Context) (int64, error)

	// Set sets the value of the counter
	Set(ctx context.Context, value int64) error

	// Increment increments the counter by the given delta
	Increment(ctx context.Context, delta int64) (int64, error)

	// Decrement decrements the counter by the given delta
	Decrement(ctx context.Context, delta int64) (int64, error)
}

// New creates a new counter for the given partitions
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (Counter, error) {
	options := newCounterOptions{}
	for _, opt := range opts {
		if op, ok := opt.(Option); ok {
			op.applyNewCounter(&options)
		}
	}
	sessions := api.NewCounterSessionClient(conn)
	request := &api.OpenSessionRequest{
		Options: options.sessionOptions,
	}
	response, err := sessions.OpenSession(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return &counter{
		Client:  primitive.NewClient(Type, name, response.SessionID),
		client:  api.NewCounterClient(conn),
		session: sessions,
	}, nil
}

// counter is the single partition implementation of Counter
type counter struct {
	*primitive.Client
	client  api.CounterClient
	session api.CounterSessionClient
}

func (c *counter) Get(ctx context.Context) (int64, error) {
	request := &api.GetRequest{}
	response, err := c.client.Get(c.GetContext(ctx), request)
	if err != nil {
		return 0, errors.From(err)
	}
	return response.Value, nil
}

func (c *counter) Set(ctx context.Context, value int64) error {
	request := &api.SetRequest{
		Value: value,
	}
	_, err := c.client.Set(c.GetContext(ctx), request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (c *counter) Increment(ctx context.Context, delta int64) (int64, error) {
	request := &api.IncrementRequest{
		Delta: delta,
	}
	response, err := c.client.Increment(c.GetContext(ctx), request)
	if err != nil {
		return 0, errors.From(err)
	}
	return response.Value, nil
}

func (c *counter) Decrement(ctx context.Context, delta int64) (int64, error) {
	request := &api.DecrementRequest{
		Delta: delta,
	}
	response, err := c.client.Decrement(c.GetContext(ctx), request)
	if err != nil {
		return 0, errors.From(err)
	}
	return response.Value, nil
}

func (c *counter) Close(ctx context.Context) error {
	request := &api.CloseSessionRequest{
		SessionID: c.SessionID(),
	}
	_, err := c.session.CloseSession(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}
