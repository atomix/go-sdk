// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/go-client/pkg/primitive"
	counterv1 "github.com/atomix/runtime/api/atomix/runtime/counter/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
)

// AtomicCounter provides a distributed atomic counter
type AtomicCounter interface {
	primitive.Primitive

	// Get gets the current value of the counter
	Get(ctx context.Context) (int64, error)

	// Set sets the value of the counter
	Set(ctx context.Context, value int64) error

	// Update updates the value of the counter
	Update(ctx context.Context, current, update int64) error

	// Increment increments the counter by the given delta
	Increment(ctx context.Context, delta int64) (int64, error)

	// Decrement decrements the counter by the given delta
	Decrement(ctx context.Context, delta int64) (int64, error)
}

// counter is the single partition implementation of AtomicCounter
type counterPrimitive struct {
	primitive.Primitive
	client counterv1.CounterClient
}

func (c *counterPrimitive) Get(ctx context.Context) (int64, error) {
	request := &counterv1.GetRequest{
		ID: primitivev1.PrimitiveId{
			Name: c.Name(),
		},
	}
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return response.Value, nil
}

func (c *counterPrimitive) Set(ctx context.Context, value int64) error {
	request := &counterv1.SetRequest{
		ID: primitivev1.PrimitiveId{
			Name: c.Name(),
		},
		Value: value,
	}
	_, err := c.client.Set(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (c *counterPrimitive) Update(ctx context.Context, current, update int64) error {
	request := &counterv1.UpdateRequest{
		ID: primitivev1.PrimitiveId{
			Name: c.Name(),
		},
		Check:  current,
		Update: update,
	}
	_, err := c.client.Update(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (c *counterPrimitive) Increment(ctx context.Context, delta int64) (int64, error) {
	request := &counterv1.IncrementRequest{
		ID: primitivev1.PrimitiveId{
			Name: c.Name(),
		},
		Delta: delta,
	}
	response, err := c.client.Increment(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return response.Value, nil
}

func (c *counterPrimitive) Decrement(ctx context.Context, delta int64) (int64, error) {
	request := &counterv1.DecrementRequest{
		ID: primitivev1.PrimitiveId{
			Name: c.Name(),
		},
		Delta: delta,
	}
	response, err := c.client.Decrement(ctx, request)
	if err != nil {
		return 0, errors.FromProto(err)
	}
	return response.Value, nil
}

func (c *counterPrimitive) create(ctx context.Context, tags ...string) error {
	request := &counterv1.CreateRequest{
		ID: primitivev1.PrimitiveId{
			Name: c.Name(),
		},
		Tags: tags,
	}
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
	request := &counterv1.CloseRequest{
		ID: primitivev1.PrimitiveId{
			Name: c.Name(),
		},
	}
	_, err := c.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
