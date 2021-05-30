// Copyright 2020-present Open Networking Foundation.
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

package test

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/atomix/counter"
	"github.com/atomix/atomix-go-client/pkg/atomix/election"
	"github.com/atomix/atomix-go-client/pkg/atomix/indexedmap"
	"github.com/atomix/atomix-go-client/pkg/atomix/list"
	"github.com/atomix/atomix-go-client/pkg/atomix/lock"
	_map "github.com/atomix/atomix-go-client/pkg/atomix/map"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/set"
	"github.com/atomix/atomix-go-client/pkg/atomix/value"
	"google.golang.org/grpc"
)

type Client interface {
	Start(driverPort, agentPort int) error
	Connect(ctx context.Context, primitive primitive.Type, name string) (*grpc.ClientConn, error)
	Stop() error
}

func newClient(id string, client Client) *testClient {
	return &testClient{
		Client: client,
		id:     id,
	}
}

type testClient struct {
	Client
	id string
}

func (c *testClient) getOpts(opts ...primitive.Option) []primitive.Option {
	return append([]primitive.Option{primitive.WithSessionID(c.id)}, opts...)
}

func (c *testClient) GetCounter(ctx context.Context, name string, opts ...primitive.Option) (counter.Counter, error) {
	conn, err := c.Connect(ctx, counter.Type, name)
	if err != nil {
		return nil, err
	}
	return counter.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetElection(ctx context.Context, name string, opts ...primitive.Option) (election.Election, error) {
	conn, err := c.Connect(ctx, election.Type, name)
	if err != nil {
		return nil, err
	}
	return election.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetIndexedMap(ctx context.Context, name string, opts ...primitive.Option) (indexedmap.IndexedMap, error) {
	conn, err := c.Connect(ctx, indexedmap.Type, name)
	if err != nil {
		return nil, err
	}
	return indexedmap.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetList(ctx context.Context, name string, opts ...primitive.Option) (list.List, error) {
	conn, err := c.Connect(ctx, list.Type, name)
	if err != nil {
		return nil, err
	}
	return list.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetLock(ctx context.Context, name string, opts ...primitive.Option) (lock.Lock, error) {
	conn, err := c.Connect(ctx, lock.Type, name)
	if err != nil {
		return nil, err
	}
	return lock.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetMap(ctx context.Context, name string, opts ...primitive.Option) (_map.Map, error) {
	conn, err := c.Connect(ctx, _map.Type, name)
	if err != nil {
		return nil, err
	}
	return _map.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetSet(ctx context.Context, name string, opts ...primitive.Option) (set.Set, error) {
	conn, err := c.Connect(ctx, set.Type, name)
	if err != nil {
		return nil, err
	}
	return set.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetValue(ctx context.Context, name string, opts ...primitive.Option) (value.Value, error) {
	conn, err := c.Connect(ctx, value.Type, name)
	if err != nil {
		return nil, err
	}
	return value.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) Close() error {
	return c.Client.Stop()
}
