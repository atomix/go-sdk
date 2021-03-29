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
	"github.com/atomix/go-client/pkg/atomix/counter"
	"github.com/atomix/go-client/pkg/atomix/election"
	"github.com/atomix/go-client/pkg/atomix/indexedmap"
	"github.com/atomix/go-client/pkg/atomix/leader"
	"github.com/atomix/go-client/pkg/atomix/list"
	"github.com/atomix/go-client/pkg/atomix/lock"
	_map "github.com/atomix/go-client/pkg/atomix/map"
	"github.com/atomix/go-client/pkg/atomix/set"
	"github.com/atomix/go-client/pkg/atomix/value"
	"io"
)

// Namespace returns a client for the given namespace
func Namespace(namespace string) NamespaceClient {
	return getClient().Namespace(namespace)
}

// NamespaceClient is an Atomix namespace client
type NamespaceClient interface {
	counter.NamespaceClient
	election.NamespaceClient
	indexedmap.NamespaceClient
	leader.NamespaceClient
	list.NamespaceClient
	lock.NamespaceClient
	_map.NamespaceClient
	set.NamespaceClient
	value.NamespaceClient
	io.Closer
}

type namespaceClient struct {
	client    Client
	namespace string
}

func (c *namespaceClient) GetCounter(ctx context.Context, name string, opts ...counter.Option) (counter.Counter, error) {
	return c.client.GetCounter(ctx, c.namespace, name, opts...)
}

func (c *namespaceClient) GetElection(ctx context.Context, name string, opts ...election.Option) (election.Election, error) {
	return c.client.GetElection(ctx, c.namespace, name, opts...)
}

func (c *namespaceClient) GetIndexedMap(ctx context.Context, name string, opts ...indexedmap.Option) (indexedmap.IndexedMap, error) {
	return c.client.GetIndexedMap(ctx, c.namespace, name, opts...)
}

func (c *namespaceClient) GetLatch(ctx context.Context, name string, opts ...leader.Option) (leader.Latch, error) {
	return c.client.GetLatch(ctx, c.namespace, name, opts...)
}

func (c *namespaceClient) GetList(ctx context.Context, name string, opts ...list.Option) (list.List, error) {
	return c.client.GetList(ctx, c.namespace, name, opts...)
}

func (c *namespaceClient) GetLock(ctx context.Context, name string, opts ...lock.Option) (lock.Lock, error) {
	return c.client.GetLock(ctx, c.namespace, name, opts...)
}

func (c *namespaceClient) GetMap(ctx context.Context, name string, opts ..._map.Option) (_map.Map, error) {
	return c.client.GetMap(ctx, c.namespace, name, opts...)
}

func (c *namespaceClient) GetSet(ctx context.Context, name string, opts ...set.Option) (set.Set, error) {
	return c.client.GetSet(ctx, c.namespace, name, opts...)
}

func (c *namespaceClient) GetValue(ctx context.Context, name string, opts ...value.Option) (value.Value, error) {
	return c.client.GetValue(ctx, c.namespace, name, opts...)
}

func (c *namespaceClient) Close() error {
	return c.client.Close()
}

var _ NamespaceClient = &namespaceClient{}
