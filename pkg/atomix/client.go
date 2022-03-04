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
	brokerapi "github.com/atomix/atomix-api/go/atomix/management/broker"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/counter"
	"github.com/atomix/atomix-go-client/pkg/atomix/election"
	"github.com/atomix/atomix-go-client/pkg/atomix/indexedmap"
	"github.com/atomix/atomix-go-client/pkg/atomix/list"
	"github.com/atomix/atomix-go-client/pkg/atomix/lock"
	_map "github.com/atomix/atomix-go-client/pkg/atomix/map"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/set"
	"github.com/atomix/atomix-go-client/pkg/atomix/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/retry"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"io"
	"sync"
	"time"
)

// GetCounter gets the Counter instance of the given name
func GetCounter(ctx context.Context, name string, opts ...primitive.Option) (counter.Counter, error) {
	return getClient().GetCounter(ctx, name, opts...)
}

// GetElection gets the Election instance of the given name
func GetElection(ctx context.Context, name string, opts ...primitive.Option) (election.Election, error) {
	return getClient().GetElection(ctx, name, opts...)
}

// GetIndexedMap gets the IndexedMap instance of the given name
func GetIndexedMap(ctx context.Context, name string, opts ...primitive.Option) (indexedmap.IndexedMap, error) {
	return getClient().GetIndexedMap(ctx, name, opts...)
}

// GetList gets the List instance of the given name
func GetList(ctx context.Context, name string, opts ...primitive.Option) (list.List, error) {
	return getClient().GetList(ctx, name, opts...)
}

// GetLock gets the Lock instance of the given name
func GetLock(ctx context.Context, name string, opts ...primitive.Option) (lock.Lock, error) {
	return getClient().GetLock(ctx, name, opts...)
}

// GetMap gets the Map instance of the given name
func GetMap(ctx context.Context, name string, opts ...primitive.Option[_map.Map]) (_map.Map, error) {
	return getClient().GetMap(ctx, name, opts...)
}

// GetSet gets the Set instance of the given name
func GetSet(ctx context.Context, name string, opts ...primitive.Option) (set.Set, error) {
	return getClient().GetSet(ctx, name, opts...)
}

// GetValue gets the Value instance of the given name
func GetValue(ctx context.Context, name string, opts ...primitive.Option) (value.Value, error) {
	return getClient().GetValue(ctx, name, opts...)
}

// NewClient creates a new Atomix client
func NewClient(opts ...Option) Client {
	options := clientOptions{
		clientID:   uuid.New().String(),
		brokerHost: defaultHost,
		brokerPort: defaultPort,
	}
	for _, opt := range opts {
		opt.apply(&options)
	}
	return &atomixClient{
		options:        options,
		primitiveConns: make(map[primitiveapi.PrimitiveId]*grpc.ClientConn),
	}
}

// Client is an Atomix client
type Client interface {
	counter.Client
	election.Client
	indexedmap.Client
	list.Client
	lock.Client
	_map.Client
	set.Client
	value.Client
	io.Closer
}

type atomixClient struct {
	options        clientOptions
	brokerConn     *grpc.ClientConn
	primitiveConns map[primitiveapi.PrimitiveId]*grpc.ClientConn
	mu             sync.RWMutex
}

func (c *atomixClient) connect(ctx context.Context, primitive primitiveapi.PrimitiveId) (*grpc.ClientConn, error) {
	c.mu.RLock()
	driverConn, ok := c.primitiveConns[primitive]
	c.mu.RUnlock()
	if ok {
		return driverConn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	driverConn, ok = c.primitiveConns[primitive]
	if ok {
		return driverConn, nil
	}

	brokerConn := c.brokerConn
	if brokerConn == nil {
		conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", c.options.brokerHost, c.options.brokerPort),
			grpc.WithInsecure(),
			grpc.WithUnaryInterceptor(retry.RetryingUnaryClientInterceptor(retry.WithRetryOn(codes.Unavailable))))
		if err != nil {
			return nil, err
		}
		c.brokerConn = conn
		brokerConn = conn
	}

	brokerClient := brokerapi.NewBrokerClient(brokerConn)
	request := &brokerapi.LookupPrimitiveRequest{
		PrimitiveID: brokerapi.PrimitiveId{
			PrimitiveId: primitive,
		},
	}
	response, err := brokerClient.LookupPrimitive(ctx, request, retry.WithRetryOn(codes.Unavailable, codes.NotFound), retry.WithPerCallTimeout(time.Second))
	if err != nil {
		return nil, errors.From(err)
	}

	driverConn, err = grpc.DialContext(ctx, fmt.Sprintf("%s:%d", response.Address.Host, response.Address.Port),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(retry.RetryingUnaryClientInterceptor(retry.WithRetryOn(codes.Unavailable))),
		grpc.WithStreamInterceptor(retry.RetryingStreamClientInterceptor(retry.WithRetryOn(codes.Unavailable))))
	if err != nil {
		return nil, err
	}
	c.primitiveConns[primitive] = driverConn
	return driverConn, nil
}

func newPrimitiveID[T primitive.Primitive](t primitive.Type[T], name string) primitiveapi.PrimitiveId {
	return primitiveapi.PrimitiveId{
		Type: t.String(),
		Name: name,
	}
}

func getPrimitiveOpts[T primitive.Primitive](clientOpts clientOptions, primitiveOpts ...primitive.Option[T]) []primitive.Option[T] {
	return append([]primitive.Option[T]{primitive.WithSessionID[T](clientOpts.clientID)}, primitiveOpts...)
}

func (c *atomixClient) GetCounter(ctx context.Context, name string, opts ...primitive.Option) (counter.Counter, error) {
	conn, err := c.connect(ctx, newPrimitiveID(counter.Type, name))
	if err != nil {
		return nil, err
	}
	return counter.New(ctx, name, conn, getPrimitiveOpts(c.options, opts...)...)
}

func (c *atomixClient) GetElection(ctx context.Context, name string, opts ...primitive.Option) (election.Election, error) {
	conn, err := c.connect(ctx, newPrimitiveID(election.Type, name))
	if err != nil {
		return nil, err
	}
	return election.New(ctx, name, conn, getPrimitiveOpts(c.options, opts...)...)
}

func (c *atomixClient) GetIndexedMap(ctx context.Context, name string, opts ...primitive.Option) (indexedmap.IndexedMap, error) {
	conn, err := c.connect(ctx, newPrimitiveID(indexedmap.Type, name))
	if err != nil {
		return nil, err
	}
	return indexedmap.New(ctx, name, conn, getPrimitiveOpts(c.options, opts...)...)
}

func (c *atomixClient) GetList(ctx context.Context, name string, opts ...primitive.Option) (list.List, error) {
	conn, err := c.connect(ctx, newPrimitiveID(list.Type, name))
	if err != nil {
		return nil, err
	}
	return list.New(ctx, name, conn, getPrimitiveOpts(c.options, opts...)...)
}

func (c *atomixClient) GetLock(ctx context.Context, name string, opts ...primitive.Option) (lock.Lock, error) {
	conn, err := c.connect(ctx, newPrimitiveID(lock.Type, name))
	if err != nil {
		return nil, err
	}
	return lock.New(ctx, name, conn, getPrimitiveOpts(c.options, opts...)...)
}

func (c *atomixClient) GetMap(ctx context.Context, name string, opts ...primitive.Option[_map.Map]) (_map.Map, error) {
	conn, err := c.connect(ctx, newPrimitiveID(_map.Type, name))
	if err != nil {
		return nil, err
	}
	return _map.New(ctx, name, conn, getPrimitiveOpts(c.options, opts...)...)
}

func (c *atomixClient) GetSet(ctx context.Context, name string, opts ...primitive.Option) (set.Set, error) {
	conn, err := c.connect(ctx, newPrimitiveID(set.Type, name))
	if err != nil {
		return nil, err
	}
	return set.New(ctx, name, conn, getPrimitiveOpts(c.options, opts...)...)
}

func (c *atomixClient) GetValue(ctx context.Context, name string, opts ...primitive.Option) (value.Value, error) {
	conn, err := c.connect(ctx, newPrimitiveID(value.Type, name))
	if err != nil {
		return nil, err
	}
	return value.New(ctx, name, conn, getPrimitiveOpts(c.options, opts...)...)
}

func (c *atomixClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.primitiveConns {
		conn.Close()
	}
	if c.brokerConn != nil {
		return c.brokerConn.Close()
	}
	return nil
}

var _ Client = &atomixClient{}
