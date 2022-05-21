// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	counterv1 "github.com/atomix/runtime/api/atomix/counter/v1"
	"github.com/atomix/runtime/pkg/errors"
)

const serviceName = "atomix.counter.v1.Counter"

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

func Provider(client primitive.Client) primitive.Provider[Counter, Option] {
	return primitive.NewProvider[Counter, Option](func(ctx context.Context, name string, opts ...primitive.Option) func(...Option) (Counter, error) {
		return func(counterOpts ...Option) (Counter, error) {
			// Process the primitive options
			var options Options
			options.apply(counterOpts...)

			// Construct the primitive configuration
			var config counterv1.CounterConfig

			// Open the primitive connection
			base, conn, err := primitive.Open[*counterv1.CounterConfig](client)(ctx, serviceName, name, &config, opts...)
			if err != nil {
				return nil, err
			}

			// Create the primitive instance
			return &counterPrimitive{
				ManagedPrimitive: base,
				client:           counterv1.NewCounterClient(conn),
			}, nil
		}
	})
}

// counter is the single partition implementation of Counter
type counterPrimitive struct {
	*primitive.ManagedPrimitive
	client counterv1.CounterClient
}

func (c *counterPrimitive) Get(ctx context.Context) (int64, error) {
	request := &counterv1.GetRequest{
		Headers: c.GetHeaders(),
	}
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return response.Value, nil
}

func (c *counterPrimitive) Set(ctx context.Context, value int64) error {
	request := &counterv1.SetRequest{
		Headers: c.GetHeaders(),
		SetInput: counterv1.SetInput{
			Value: value,
		},
	}
	_, err := c.client.Set(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (c *counterPrimitive) Increment(ctx context.Context, delta int64) (int64, error) {
	request := &counterv1.IncrementRequest{
		Headers: c.GetHeaders(),
		IncrementInput: counterv1.IncrementInput{
			Delta: delta,
		},
	}
	response, err := c.client.Increment(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return response.Value, nil
}

func (c *counterPrimitive) Decrement(ctx context.Context, delta int64) (int64, error) {
	request := &counterv1.DecrementRequest{
		Headers: c.GetHeaders(),
		DecrementInput: counterv1.DecrementInput{
			Delta: delta,
		},
	}
	response, err := c.client.Decrement(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return response.Value, nil
}
