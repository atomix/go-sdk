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
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-client/pkg/atomix/test"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/node"
	gossipprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	gossipcounterprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/gossip/counter"
	rsmprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	counterrsm "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/counter"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	gossipproxy "github.com/atomix/go-framework/pkg/atomix/proxy/gossip"
	gossipcounterproxy "github.com/atomix/go-framework/pkg/atomix/proxy/gossip/counter"
	rsmproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	counterproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/counter"
	atime "github.com/atomix/go-framework/pkg/atomix/time"
	"github.com/atomix/go-local/pkg/atomix/local"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCounterOperations(t *testing.T) {
	test := test.NewTestCluster()
	defer test.Stop()
	test.Storage().AddPrimitive(primitiveapi.PrimitiveMeta{
		Name:   "test",
		Type:   Type.String(),
		Driver: "test",
	})
	err := test.Storage().Start(func(c cluster.Cluster) node.Node {
		node := rsmprotocol.NewNode(c, local.NewProtocol())
		counterrsm.RegisterService(node)
		return node
	})
	assert.NoError(t, err)
	conn1, err := test.Node(1).Connect(func(c cluster.Cluster) proxy.Node {
		node := rsmproxy.NewNode(c)
		counterproxy.RegisterProxy(node)
		return node
	})
	assert.NoError(t, err)
	conn2, err := test.Node(2).Connect(func(c cluster.Cluster) proxy.Node {
		node := rsmproxy.NewNode(c)
		counterproxy.RegisterProxy(node)
		return node
	})
	assert.NoError(t, err)
	conn3, err := test.Node(3).Connect(func(c cluster.Cluster) proxy.Node {
		node := rsmproxy.NewNode(c)
		counterproxy.RegisterProxy(node)
		return node
	})
	assert.NoError(t, err)

	counter, err := New(context.TODO(), "test", conn1)
	assert.NoError(t, err)
	assert.NotNil(t, counter)

	value, err := counter.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), value)

	err = counter.Set(context.TODO(), 1)
	assert.NoError(t, err)

	value, err = counter.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), value)

	err = counter.Set(context.TODO(), -1)
	assert.NoError(t, err)

	value, err = counter.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), value)

	value, err = counter.Increment(context.TODO(), 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), value)

	value, err = counter.Decrement(context.TODO(), 10)
	assert.NoError(t, err)
	assert.Equal(t, int64(-10), value)

	value, err = counter.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(-10), value)

	value, err = counter.Increment(context.TODO(), 20)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), value)

	err = counter.Close(context.Background())
	assert.NoError(t, err)

	counter1, err := New(context.TODO(), "test", conn2)
	assert.NoError(t, err)

	counter2, err := New(context.TODO(), "test", conn3)
	assert.NoError(t, err)

	value, err = counter1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(10), value)

	err = counter1.Close(context.Background())
	assert.NoError(t, err)

	err = counter1.Delete(context.Background())
	assert.NoError(t, err)

	err = counter2.Delete(context.Background())
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))

	counter, err = New(context.TODO(), "test", conn1)
	assert.NoError(t, err)

	value, err = counter.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), value)
}

func TestGossipCounter(t *testing.T) {
	test := test.NewTestCluster()
	defer test.Stop()
	test.Storage().AddPrimitive(primitiveapi.PrimitiveMeta{
		Name:   "test",
		Type:   Type.String(),
		Driver: "test",
	})
	err := test.Storage().Start(func(c cluster.Cluster) node.Node {
		node := gossipprotocol.NewNode(c, atime.LogicalScheme)
		gossipcounterprotocol.RegisterService(node)
		return node
	})
	assert.NoError(t, err)
	conn1, err := test.Node(1).Connect(func(c cluster.Cluster) proxy.Node {
		node := gossipproxy.NewNode(c, atime.LogicalScheme)
		gossipcounterproxy.RegisterProxy(node)
		return node
	})
	assert.NoError(t, err)
	conn2, err := test.Node(2).Connect(func(c cluster.Cluster) proxy.Node {
		node := gossipproxy.NewNode(c, atime.LogicalScheme)
		gossipcounterproxy.RegisterProxy(node)
		return node
	})
	assert.NoError(t, err)

	counter1, err := New(context.TODO(), "test", conn1)
	assert.NoError(t, err)
	assert.NotNil(t, counter1)

	counter2, err := New(context.TODO(), "test", conn2)
	assert.NoError(t, err)
	assert.NotNil(t, counter2)

	value, err := counter1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), value)

	value, err = counter2.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), value)

	value, err = counter1.Increment(context.TODO(), 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), value)

	value, err = counter1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), value)

	value, err = counter2.Get(context.TODO())
	assert.NoError(t, err)

	time.Sleep(time.Second)

	value, err = counter2.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), value)

	value, err = counter2.Decrement(context.TODO(), 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(-4), value)

	value, err = counter2.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(-4), value)

	time.Sleep(time.Second)

	value, err = counter1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(-4), value)

	err = counter1.Set(context.TODO(), 10)
	assert.NoError(t, err)

	value, err = counter1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(10), value)

	time.Sleep(time.Second)

	value, err = counter2.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(10), value)
}
