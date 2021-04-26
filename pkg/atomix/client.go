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
	brokerapi "github.com/atomix/api/go/atomix/management/broker"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-client/pkg/atomix/counter"
	"github.com/atomix/go-client/pkg/atomix/election"
	"github.com/atomix/go-client/pkg/atomix/indexedmap"
	"github.com/atomix/go-client/pkg/atomix/leader"
	"github.com/atomix/go-client/pkg/atomix/list"
	"github.com/atomix/go-client/pkg/atomix/lock"
	_map "github.com/atomix/go-client/pkg/atomix/map"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"github.com/atomix/go-client/pkg/atomix/set"
	"github.com/atomix/go-client/pkg/atomix/value"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"google.golang.org/grpc"
	"io"
	"sync"
)

// GetCounter gets the Counter instance of the given name
func GetCounter(ctx context.Context, namespace string, name string, opts ...counter.Option) (counter.Counter, error) {
	return getClient().GetCounter(ctx, namespace, name, opts...)
}

// GetElection gets the Election instance of the given name
func GetElection(ctx context.Context, namespace string, name string, opts ...election.Option) (election.Election, error) {
	return getClient().GetElection(ctx, namespace, name, opts...)
}

// GetIndexedMap gets the IndexedMap instance of the given name
func GetIndexedMap(ctx context.Context, namespace string, name string, opts ...indexedmap.Option) (indexedmap.IndexedMap, error) {
	return getClient().GetIndexedMap(ctx, namespace, name, opts...)
}

// GetLatch gets the Latch instance of the given name
func GetLatch(ctx context.Context, namespace string, name string, opts ...leader.Option) (leader.Latch, error) {
	return getClient().GetLatch(ctx, namespace, name, opts...)
}

// GetList gets the List instance of the given name
func GetList(ctx context.Context, namespace string, name string, opts ...list.Option) (list.List, error) {
	return getClient().GetList(ctx, namespace, name, opts...)
}

// GetLock gets the Lock instance of the given name
func GetLock(ctx context.Context, namespace string, name string, opts ...lock.Option) (lock.Lock, error) {
	return getClient().GetLock(ctx, namespace, name, opts...)
}

// GetMap gets the Map instance of the given name
func GetMap(ctx context.Context, namespace string, name string, opts ..._map.Option) (_map.Map, error) {
	return getClient().GetMap(ctx, namespace, name, opts...)
}

// GetSet gets the Set instance of the given name
func GetSet(ctx context.Context, namespace string, name string, opts ...set.Option) (set.Set, error) {
	return getClient().GetSet(ctx, namespace, name, opts...)
}

// GetValue gets the Value instance of the given name
func GetValue(ctx context.Context, namespace string, name string, opts ...value.Option) (value.Value, error) {
	return getClient().GetValue(ctx, namespace, name, opts...)
}

// NewClient creates a new Atomix client
func NewClient(opts ...Option) Client {
	return &atomixClient{
		options:        newOptions(opts...),
		primitiveConns: make(map[primitiveapi.PrimitiveId]*grpc.ClientConn),
	}
}

// Client is an Atomix client
type Client interface {
	Namespace(namespace string) NamespaceClient
	counter.ClusterClient
	election.ClusterClient
	indexedmap.ClusterClient
	leader.ClusterClient
	list.ClusterClient
	lock.ClusterClient
	_map.ClusterClient
	set.ClusterClient
	value.ClusterClient
	io.Closer
}

type atomixClient struct {
	options        Options
	brokerConn     *grpc.ClientConn
	primitiveConns map[primitiveapi.PrimitiveId]*grpc.ClientConn
	mu             sync.RWMutex
}

func (c *atomixClient) Namespace(namespace string) NamespaceClient {
	return &namespaceClient{
		client:    c,
		namespace: namespace,
	}
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
		conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", c.options.Host, c.options.Port), grpc.WithInsecure())
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
	response, err := brokerClient.LookupPrimitive(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}

	driverConn, err = grpc.DialContext(ctx, fmt.Sprintf("%s:%d", response.Address.Host, response.Address.Port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c.primitiveConns[primitive] = driverConn
	return driverConn, nil
}

func newPrimitiveID(t primitive.Type, namespace, name string) primitiveapi.PrimitiveId {
	return primitiveapi.PrimitiveId{
		Type:      t.String(),
		Namespace: namespace,
		Name:      name,
	}
}

func (c *atomixClient) GetCounter(ctx context.Context, namespace, name string, opts ...counter.Option) (counter.Counter, error) {
	conn, err := c.connect(ctx, newPrimitiveID(counter.Type, namespace, name))
	if err != nil {
		return nil, err
	}
	return counter.New(ctx, namespace, name, conn, opts...)
}

func (c *atomixClient) GetElection(ctx context.Context, namespace, name string, opts ...election.Option) (election.Election, error) {
	conn, err := c.connect(ctx, newPrimitiveID(election.Type, namespace, name))
	if err != nil {
		return nil, err
	}
	return election.New(ctx, namespace, name, conn, opts...)
}

func (c *atomixClient) GetIndexedMap(ctx context.Context, namespace, name string, opts ...indexedmap.Option) (indexedmap.IndexedMap, error) {
	conn, err := c.connect(ctx, newPrimitiveID(indexedmap.Type, namespace, name))
	if err != nil {
		return nil, err
	}
	return indexedmap.New(ctx, namespace, name, conn, opts...)
}

func (c *atomixClient) GetLatch(ctx context.Context, namespace, name string, opts ...leader.Option) (leader.Latch, error) {
	conn, err := c.connect(ctx, newPrimitiveID(leader.Type, namespace, name))
	if err != nil {
		return nil, err
	}
	return leader.New(ctx, namespace, name, conn, opts...)
}

func (c *atomixClient) GetList(ctx context.Context, namespace, name string, opts ...list.Option) (list.List, error) {
	conn, err := c.connect(ctx, newPrimitiveID(list.Type, namespace, name))
	if err != nil {
		return nil, err
	}
	return list.New(ctx, namespace, name, conn, opts...)
}

func (c *atomixClient) GetLock(ctx context.Context, namespace, name string, opts ...lock.Option) (lock.Lock, error) {
	conn, err := c.connect(ctx, newPrimitiveID(lock.Type, namespace, name))
	if err != nil {
		return nil, err
	}
	return lock.New(ctx, namespace, name, conn, opts...)
}

func (c *atomixClient) GetMap(ctx context.Context, namespace, name string, opts ..._map.Option) (_map.Map, error) {
	conn, err := c.connect(ctx, newPrimitiveID(_map.Type, namespace, name))
	if err != nil {
		return nil, err
	}
	return _map.New(ctx, namespace, name, conn, opts...)
}

func (c *atomixClient) GetSet(ctx context.Context, namespace, name string, opts ...set.Option) (set.Set, error) {
	conn, err := c.connect(ctx, newPrimitiveID(set.Type, namespace, name))
	if err != nil {
		return nil, err
	}
	return set.New(ctx, namespace, name, conn, opts...)
}

func (c *atomixClient) GetValue(ctx context.Context, namespace, name string, opts ...value.Option) (value.Value, error) {
	conn, err := c.connect(ctx, newPrimitiveID(value.Type, namespace, name))
	if err != nil {
		return nil, err
	}
	return value.New(ctx, namespace, name, conn, opts...)
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
