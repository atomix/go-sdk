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
	api "github.com/atomix/atomix-api/go/atomix/primitive/counter"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "client", "counter")

// Type is the counter type
const Type primitive.Type = "Counter"

// Client provides an API for creating Counters
type Client interface {
	// GetCounter gets the Counter instance of the given name
	GetCounter(ctx context.Context, name string, opts ...primitive.Option) (Counter, error)
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
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...primitive.Option) (Counter, error) {
	options := newCounterOptions{}
	for _, opt := range opts {
		if op, ok := opt.(Option); ok {
			op.applyNewCounter(&options)
		}
	}
	c := &counter{
		Client:  primitive.NewClient(Type, name, conn, opts...),
		client:  api.NewCounterServiceClient(conn),
		options: options,
	}
	if err := c.Create(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// counter is the single partition implementation of Counter
type counter struct {
	*primitive.Client
	client  api.CounterServiceClient
	options newCounterOptions
}

func (c *counter) Get(ctx context.Context) (int64, error) {
	request := &api.GetRequest{
		Headers: c.GetHeaders(),
	}
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return response.Value, nil
}

func (c *counter) Set(ctx context.Context, value int64) error {
	request := &api.SetRequest{
		Headers: c.GetHeaders(),
		Value:   value,
	}
	_, err := c.client.Set(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (c *counter) Increment(ctx context.Context, delta int64) (int64, error) {
	request := &api.IncrementRequest{
		Headers: c.GetHeaders(),
		Delta:   delta,
	}
	response, err := c.client.Increment(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return response.Value, nil
}

func (c *counter) Decrement(ctx context.Context, delta int64) (int64, error) {
	request := &api.DecrementRequest{
		Headers: c.GetHeaders(),
		Delta:   delta,
	}
	response, err := c.client.Decrement(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return response.Value, nil
}
