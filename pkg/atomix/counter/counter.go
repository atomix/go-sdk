// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	counterv1 "github.com/atomix/runtime/api/atomix/counter/v1"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc"
)

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

func Client(conn *grpc.ClientConn) primitive.Client[Counter, Option] {
	return primitive.NewClient[Counter, Option](newManager(conn), func(primitive *primitive.ManagedPrimitive, opts ...Option) (Counter, error) {
		return &counter{
			ManagedPrimitive: primitive,
			client:           counterv1.NewCounterClient(conn),
		}, nil
	})
}

// counter is the single partition implementation of Counter
type counter struct {
	*primitive.ManagedPrimitive
	client counterv1.CounterClient
}

func (c *counter) Get(ctx context.Context) (int64, error) {
	request := &counterv1.GetRequest{
		Headers: c.GetHeaders(),
	}
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return response.Value, nil
}

func (c *counter) Set(ctx context.Context, value int64) error {
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

func (c *counter) Increment(ctx context.Context, delta int64) (int64, error) {
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

func (c *counter) Decrement(ctx context.Context, delta int64) (int64, error) {
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
