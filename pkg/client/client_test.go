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

package client

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/counter"
	"github.com/atomix/atomix-go-client/pkg/client/election"
	"github.com/atomix/atomix-go-client/pkg/client/list"
	"github.com/atomix/atomix-go-client/pkg/client/lock"
	"github.com/atomix/atomix-go-client/pkg/client/map"
	"github.com/atomix/atomix-go-client/pkg/client/set"
	"github.com/atomix/atomix-go-client/pkg/client/test"
	"github.com/atomix/atomix-go-client/pkg/client/value"
	"github.com/atomix/atomix-go-local/pkg/atomix/local"
	"github.com/atomix/atomix-go-node/pkg/atomix/registry"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClient(t *testing.T) {
	controller := local.NewController(5679, registry.Registry)
	err := controller.Start()
	assert.NoError(t, err)
	defer controller.Stop()

	client, err := NewClient("localhost:5679", WithNamespace("default"), WithApplication("test"))
	assert.NoError(t, err)
	assert.NotNil(t, client)

	groups, err := client.GetGroups(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, groups, 0)

	_, err = client.GetGroup(context.TODO(), "none")
	assert.EqualError(t, err, "unknown partition group none")

	group, err := client.CreateGroup(context.TODO(), "test", 3, 1, &empty.Empty{})
	assert.NoError(t, err)
	assert.NotNil(t, group)
	assert.Equal(t, "default", group.Namespace)
	assert.Equal(t, "test", group.Name)
	assert.Equal(t, 3, group.Partitions)
	assert.Equal(t, 1, group.PartitionSize)

	group, err = client.GetGroup(context.TODO(), "test")
	assert.NoError(t, err)
	assert.NotNil(t, group)
	assert.Equal(t, "default", group.Namespace)
	assert.Equal(t, "test", group.Name)
	assert.Equal(t, 3, group.Partitions)
	assert.Equal(t, 1, group.PartitionSize)

	groups, err = client.GetGroups(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, groups, 1)
	group = groups[0]
	assert.Equal(t, "default", group.Namespace)
	assert.Equal(t, "test", group.Name)
	assert.Equal(t, 3, group.Partitions)
	assert.Equal(t, 1, group.PartitionSize)

	val, err := group.GetValue(context.TODO(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, val)
	_, err = val.Set(context.TODO(), []byte("bar"))
	assert.NoError(t, err)
	_, _, err = val.Get(context.TODO())
	assert.NoError(t, err)

	err = client.DeleteGroup(context.TODO(), "test")
	assert.NoError(t, err)

	_, err = client.GetGroup(context.TODO(), "test")
	assert.EqualError(t, err, "unknown partition group test")

	groups, err = client.GetGroups(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, groups, 0)

	client.Close()
}

func TestPartitionGroup(t *testing.T) {
	conns, partitions := test.StartTestPartitions(3)

	group := &PartitionGroup{
		Namespace:     "default",
		Name:          "test",
		Partitions:    len(conns),
		PartitionSize: 1,
		Protocol:      "local",
		application:   "default",
		partitions:    conns,
	}

	primitives, err := group.GetPrimitives(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, primitives, 0)

	_, err = group.GetCounter(context.TODO(), "counter")
	assert.NoError(t, err)

	primitives, err = group.GetPrimitives(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, primitives, 1)

	primitives, err = group.GetPrimitives(context.TODO(), counter.Type)
	assert.NoError(t, err)
	assert.Len(t, primitives, 1)

	_, err = group.GetElection(context.TODO(), "election")
	assert.NoError(t, err)

	primitives, err = group.GetPrimitives(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, primitives, 2)

	primitives, err = group.GetPrimitives(context.TODO(), election.Type)
	assert.NoError(t, err)
	assert.Len(t, primitives, 1)

	_, err = group.GetList(context.TODO(), "list")
	assert.NoError(t, err)

	primitives, err = group.GetPrimitives(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, primitives, 3)

	primitives, err = group.GetPrimitives(context.TODO(), list.Type)
	assert.NoError(t, err)
	assert.Len(t, primitives, 1)

	_, err = group.GetLock(context.TODO(), "lock")
	assert.NoError(t, err)

	primitives, err = group.GetPrimitives(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, primitives, 4)

	primitives, err = group.GetPrimitives(context.TODO(), lock.Type)
	assert.NoError(t, err)
	assert.Len(t, primitives, 1)

	_, err = group.GetMap(context.TODO(), "map")
	assert.NoError(t, err)

	primitives, err = group.GetPrimitives(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, primitives, 5)

	primitives, err = group.GetPrimitives(context.TODO(), _map.Type)
	assert.NoError(t, err)
	assert.Len(t, primitives, 1)

	_, err = group.GetSet(context.TODO(), "set")
	assert.NoError(t, err)

	primitives, err = group.GetPrimitives(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, primitives, 6)

	primitives, err = group.GetPrimitives(context.TODO(), set.Type)
	assert.NoError(t, err)
	assert.Len(t, primitives, 1)

	_, err = group.GetValue(context.TODO(), "value")
	assert.NoError(t, err)

	primitives, err = group.GetPrimitives(context.TODO())
	assert.NoError(t, err)
	assert.Len(t, primitives, 7)

	primitives, err = group.GetPrimitives(context.TODO(), value.Type)
	assert.NoError(t, err)
	assert.Len(t, primitives, 1)

	test.StopTestPartitions(partitions)
}
