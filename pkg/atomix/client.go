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

package atomix

import (
	"context"
	"fmt"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-client/pkg/atomix/counter"
	"github.com/atomix/go-client/pkg/atomix/election"
	"github.com/atomix/go-client/pkg/atomix/indexedmap"
	"github.com/atomix/go-client/pkg/atomix/leader"
	"github.com/atomix/go-client/pkg/atomix/list"
	"github.com/atomix/go-client/pkg/atomix/lock"
	_map "github.com/atomix/go-client/pkg/atomix/map"
	"github.com/atomix/go-client/pkg/atomix/set"
	"github.com/atomix/go-client/pkg/atomix/value"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"google.golang.org/grpc"
	"io"
	"sync"
)

// GetCounter gets the Counter instance of the given name
func GetCounter(ctx context.Context, name string, opts ...counter.Option) (counter.Counter, error) {
	return getClient().GetCounter(ctx, name, opts...)
}

// GetElection gets the Election instance of the given name
func GetElection(ctx context.Context, name string, opts ...election.Option) (election.Election, error) {
	return getClient().GetElection(ctx, name, opts...)
}

// GetIndexedMap gets the IndexedMap instance of the given name
func GetIndexedMap(ctx context.Context, name string, opts ...indexedmap.Option) (indexedmap.IndexedMap, error) {
	return getClient().GetIndexedMap(ctx, name, opts...)
}

// GetLatch gets the Latch instance of the given name
func GetLatch(ctx context.Context, name string, opts ...leader.Option) (leader.Latch, error) {
	return getClient().GetLatch(ctx, name, opts...)
}

// GetList gets the List instance of the given name
func GetList(ctx context.Context, name string, opts ...list.Option) (list.List, error) {
	return getClient().GetList(ctx, name, opts...)
}

// GetLock gets the Lock instance of the given name
func GetLock(ctx context.Context, name string, opts ...lock.Option) (lock.Lock, error) {
	return getClient().GetLock(ctx, name, opts...)
}

// GetMap gets the Map instance of the given name
func GetMap(ctx context.Context, name string, opts ..._map.Option) (_map.Map, error) {
	return getClient().GetMap(ctx, name, opts...)
}

// GetSet gets the Set instance of the given name
func GetSet(ctx context.Context, name string, opts ...set.Option) (set.Set, error) {
	return getClient().GetSet(ctx, name, opts...)
}

// GetValue gets the Value instance of the given name
func GetValue(ctx context.Context, name string, opts ...value.Option) (value.Value, error) {
	return getClient().GetValue(ctx, name, opts...)
}

// NewClient creates a new Atomix client
func NewClient(opts ...Option) Client {
	var options Options
	options.apply(opts...)
	return &atomixClient{
		options: options,
		proxies: make(map[string]*grpc.ClientConn),
	}
}

// Client is an Atomix client
type Client interface {
	counter.Client
	election.Client
	indexedmap.Client
	leader.Client
	list.Client
	lock.Client
	_map.Client
	set.Client
	value.Client
	io.Closer
}

type atomixClient struct {
	options Options
	conn    *grpc.ClientConn
	proxies map[string]*grpc.ClientConn
	mu      sync.RWMutex
}

func (c *atomixClient) connect(ctx context.Context, name string) (*grpc.ClientConn, error) {
	c.mu.RLock()
	proxyConn, ok := c.proxies[name]
	c.mu.RUnlock()
	if ok {
		return proxyConn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	proxyConn, ok = c.proxies[name]
	if ok {
		return proxyConn, nil
	}

	coordConn := c.conn
	if coordConn == nil {
		conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", c.options.Host, c.options.Port), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		c.conn = conn
		coordConn = conn
	}

	primitiveClient := primitiveapi.NewPrimitiveRegistryServiceClient(coordConn)
	request := &primitiveapi.LookupPrimitiveRequest{
		Name: name,
	}
	response, err := primitiveClient.LookupPrimitive(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}

	proxyConn, err = grpc.DialContext(ctx, fmt.Sprintf("%s:%d", response.Proxy.Host, response.Proxy.Port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c.proxies[name] = proxyConn
	return proxyConn, nil
}

func (c *atomixClient) GetCounter(ctx context.Context, name string, opts ...counter.Option) (counter.Counter, error) {
	conn, err := c.connect(ctx, name)
	if err != nil {
		return nil, err
	}
	return counter.New(ctx, name, conn, opts...)
}

func (c *atomixClient) GetElection(ctx context.Context, name string, opts ...election.Option) (election.Election, error) {
	conn, err := c.connect(ctx, name)
	if err != nil {
		return nil, err
	}
	return election.New(ctx, name, conn, opts...)
}

func (c *atomixClient) GetIndexedMap(ctx context.Context, name string, opts ...indexedmap.Option) (indexedmap.IndexedMap, error) {
	conn, err := c.connect(ctx, name)
	if err != nil {
		return nil, err
	}
	return indexedmap.New(ctx, name, conn, opts...)
}

func (c *atomixClient) GetLatch(ctx context.Context, name string, opts ...leader.Option) (leader.Latch, error) {
	conn, err := c.connect(ctx, name)
	if err != nil {
		return nil, err
	}
	return leader.New(ctx, name, conn, opts...)
}

func (c *atomixClient) GetList(ctx context.Context, name string, opts ...list.Option) (list.List, error) {
	conn, err := c.connect(ctx, name)
	if err != nil {
		return nil, err
	}
	return list.New(ctx, name, conn, opts...)
}

func (c *atomixClient) GetLock(ctx context.Context, name string, opts ...lock.Option) (lock.Lock, error) {
	conn, err := c.connect(ctx, name)
	if err != nil {
		return nil, err
	}
	return lock.New(ctx, name, conn, opts...)
}

func (c *atomixClient) GetMap(ctx context.Context, name string, opts ..._map.Option) (_map.Map, error) {
	conn, err := c.connect(ctx, name)
	if err != nil {
		return nil, err
	}
	return _map.New(ctx, name, conn, opts...)
}

func (c *atomixClient) GetSet(ctx context.Context, name string, opts ...set.Option) (set.Set, error) {
	conn, err := c.connect(ctx, name)
	if err != nil {
		return nil, err
	}
	return set.New(ctx, name, conn, opts...)
}

func (c *atomixClient) GetValue(ctx context.Context, name string, opts ...value.Option) (value.Value, error) {
	conn, err := c.connect(ctx, name)
	if err != nil {
		return nil, err
	}
	return value.New(ctx, name, conn, opts...)
}

func (c *atomixClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.proxies {
		conn.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

var _ Client = &atomixClient{}
