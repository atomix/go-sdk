// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	counterv1 "github.com/atomix/runtime/api/atomix/counter/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
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

func New(client counterv1.CounterClient) func(context.Context, primitive.ID, ...Option) (Counter, error) {
	return func(ctx context.Context, id primitive.ID, opts ...Option) (Counter, error) {
		var options Options
		options.apply(opts...)
		counter := &counterPrimitive{
			Primitive: primitive.New(id),
			client:    client,
		}
		if err := counter.create(ctx); err != nil {
			return nil, err
		}
		return counter, nil
	}
}

// counter is the single partition implementation of Counter
type counterPrimitive struct {
	primitive.Primitive
	client counterv1.CounterClient
}

func (c *counterPrimitive) Get(ctx context.Context) (int64, error) {
	request := &counterv1.GetRequest{}
	ctx = primitive.AppendToOutgoingContext(ctx, c.ID())
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return response.Value, nil
}

func (c *counterPrimitive) Set(ctx context.Context, value int64) error {
	request := &counterv1.SetRequest{
		Value: value,
	}
	ctx = primitive.AppendToOutgoingContext(ctx, c.ID())
	_, err := c.client.Set(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (c *counterPrimitive) Increment(ctx context.Context, delta int64) (int64, error) {
	request := &counterv1.IncrementRequest{
		Delta: delta,
	}
	ctx = primitive.AppendToOutgoingContext(ctx, c.ID())
	response, err := c.client.Increment(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return response.Value, nil
}

func (c *counterPrimitive) Decrement(ctx context.Context, delta int64) (int64, error) {
	request := &counterv1.DecrementRequest{
		Delta: delta,
	}
	ctx = primitive.AppendToOutgoingContext(ctx, c.ID())
	response, err := c.client.Decrement(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return response.Value, nil
}

func (c *counterPrimitive) create(ctx context.Context) error {
	request := &counterv1.CreateRequest{
		Config: counterv1.CounterConfig{},
	}
	ctx = primitive.AppendToOutgoingContext(ctx, c.ID())
	_, err := c.client.Create(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (c *counterPrimitive) Close(ctx context.Context) error {
	request := &counterv1.CloseRequest{}
	ctx = primitive.AppendToOutgoingContext(ctx, c.ID())
	_, err := c.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
