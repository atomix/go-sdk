// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	counterv1 "github.com/atomix/atomix/api/runtime/counter/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
)

func newCountersClient(name string, client counterv1.CountersClient) primitive.Primitive {
	return &countersClient{
		name:   name,
		client: client,
	}
}

type countersClient struct {
	name   string
	client counterv1.CountersClient
}

func (s *countersClient) Name() string {
	return s.name
}

func (s *countersClient) Close(ctx context.Context) error {
	_, err := s.client.Close(ctx, &counterv1.CloseRequest{
		ID: runtimev1.PrimitiveID{
			Name: s.name,
		},
	})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func newCounterClient(name string, client counterv1.CounterClient) Counter {
	return &counterClient{
		Primitive: newCountersClient(name, client),
		client:    client,
	}
}

// counter is the single partition implementation of Counter
type counterClient struct {
	primitive.Primitive
	client counterv1.CounterClient
}

func (c *counterClient) Get(ctx context.Context) (int64, error) {
	request := &counterv1.GetRequest{
		ID: runtimev1.PrimitiveID{
			Name: c.Name(),
		},
	}
	response, err := c.client.Get(ctx, request)
	if err != nil {
		return 0, err
	}
	return response.Value, nil
}

func (c *counterClient) Set(ctx context.Context, value int64) error {
	request := &counterv1.SetRequest{
		ID: runtimev1.PrimitiveID{
			Name: c.Name(),
		},
		Value: value,
	}
	_, err := c.client.Set(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (c *counterClient) Update(ctx context.Context, current, update int64) error {
	request := &counterv1.UpdateRequest{
		ID: runtimev1.PrimitiveID{
			Name: c.Name(),
		},
		Check:  current,
		Update: update,
	}
	_, err := c.client.Update(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (c *counterClient) Increment(ctx context.Context, delta int64) (int64, error) {
	request := &counterv1.IncrementRequest{
		ID: runtimev1.PrimitiveID{
			Name: c.Name(),
		},
		Delta: delta,
	}
	response, err := c.client.Increment(ctx, request)
	if err != nil {
		return 0, err
	}
	return response.Value, nil
}

func (c *counterClient) Decrement(ctx context.Context, delta int64) (int64, error) {
	request := &counterv1.DecrementRequest{
		ID: runtimev1.PrimitiveID{
			Name: c.Name(),
		},
		Delta: delta,
	}
	response, err := c.client.Decrement(ctx, request)
	if err != nil {
		return 0, err
	}
	return response.Value, nil
}
