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
	api "github.com/atomix/atomix-api/proto/atomix/counter"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"google.golang.org/grpc"
)

type CounterClient interface {
	GetCounter(ctx context.Context, name string, opts ...session.SessionOption) (Counter, error)
}

// Counter is the interface for the counter primitive
type Counter interface {
	primitive.Primitive
	Get(ctx context.Context) (int64, error)
	Set(ctx context.Context, value int64) error
	Increment(ctx context.Context, delta int64) (int64, error)
	Decrement(ctx context.Context, delta int64) (int64, error)
}

func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.SessionOption) (Counter, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}

	client := api.NewCounterServiceClient(partitions[i])
	sess, err := session.New(ctx, name, &SessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}

	return &counter{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

type counter struct {
	name    primitive.Name
	client  api.CounterServiceClient
	session *session.Session
}

func (c *counter) Name() primitive.Name {
	return c.name
}

func (c *counter) Get(ctx context.Context) (int64, error) {
	request := &api.GetRequest{
		Header: c.session.GetRequest(),
	}

	response, err := c.client.Get(ctx, request)
	if err != nil {
		return 0, err
	}

	c.session.RecordResponse(request.Header, response.Header)
	return response.Value, nil
}

func (c *counter) Set(ctx context.Context, value int64) error {
	request := &api.SetRequest{
		Header: c.session.NextRequest(),
		Value:  value,
	}

	response, err := c.client.Set(ctx, request)
	if err != nil {
		return err
	}

	c.session.RecordResponse(request.Header, response.Header)
	return nil
}

func (c *counter) Increment(ctx context.Context, delta int64) (int64, error) {
	request := &api.IncrementRequest{
		Header: c.session.NextRequest(),
		Delta:  delta,
	}

	response, err := c.client.Increment(ctx, request)
	if err != nil {
		return 0, err
	}

	c.session.RecordResponse(request.Header, response.Header)
	return response.NextValue, nil
}

func (c *counter) Decrement(ctx context.Context, delta int64) (int64, error) {
	request := &api.DecrementRequest{
		Header: c.session.NextRequest(),
		Delta:  delta,
	}

	response, err := c.client.Decrement(ctx, request)
	if err != nil {
		return 0, err
	}

	c.session.RecordResponse(request.Header, response.Header)
	return response.NextValue, nil
}

func (c *counter) Close() error {
	return c.session.Close()
}

func (c *counter) Delete() error {
	return c.session.Delete()
}
