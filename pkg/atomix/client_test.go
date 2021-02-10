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
	driverapi "github.com/atomix/api/go/atomix/driver"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	proxyapi "github.com/atomix/api/go/atomix/proxy"
	_map "github.com/atomix/go-client/pkg/atomix/map"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/coordinator"
	gossipprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	gossipcounterprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/gossip/counter"
	gossipmapprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/gossip/map"
	gossipsetprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/gossip/set"
	gossipvalueprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/gossip/value"
	rsmprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	rsmcounterprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/counter"
	rsmelectionprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/election"
	rsmindexedmapprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/indexedmap"
	rsmleaderprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/leader"
	rsmlistprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/list"
	rsmlockprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/lock"
	rsmlogprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/log"
	rsmmapprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/map"
	rsmsetprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/set"
	rsmvalueprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/value"
	cachedmap "github.com/atomix/go-framework/pkg/atomix/proxy/cache/map"
	gossipproxy "github.com/atomix/go-framework/pkg/atomix/proxy/gossip"
	gossipcounterproxy "github.com/atomix/go-framework/pkg/atomix/proxy/gossip/counter"
	gossipmapproxy "github.com/atomix/go-framework/pkg/atomix/proxy/gossip/map"
	gossipsetproxy "github.com/atomix/go-framework/pkg/atomix/proxy/gossip/set"
	gossipvalueproxy "github.com/atomix/go-framework/pkg/atomix/proxy/gossip/value"
	romap "github.com/atomix/go-framework/pkg/atomix/proxy/ro/map"
	rsmproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	rsmcounterproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/counter"
	rsmelectionproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/election"
	rsmindexedmapproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/indexedmap"
	rsmleaderproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/leader"
	rsmlistproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/list"
	rsmlockproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/lock"
	rsmlogproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/log"
	rsmmapproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/map"
	rsmsetproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/set"
	rsmvalueproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/value"
	counterproxyserver "github.com/atomix/go-framework/pkg/atomix/proxy/server/counter"
	electionproxyserver "github.com/atomix/go-framework/pkg/atomix/proxy/server/election"
	indexedmapproxyserver "github.com/atomix/go-framework/pkg/atomix/proxy/server/indexedmap"
	leaderproxyserver "github.com/atomix/go-framework/pkg/atomix/proxy/server/leader"
	listproxyserver "github.com/atomix/go-framework/pkg/atomix/proxy/server/list"
	lockproxyserver "github.com/atomix/go-framework/pkg/atomix/proxy/server/lock"
	logproxyserver "github.com/atomix/go-framework/pkg/atomix/proxy/server/log"
	mapproxyserver "github.com/atomix/go-framework/pkg/atomix/proxy/server/map"
	setproxyserver "github.com/atomix/go-framework/pkg/atomix/proxy/server/set"
	valueproxyserver "github.com/atomix/go-framework/pkg/atomix/proxy/server/value"
	"github.com/atomix/go-framework/pkg/atomix/time"
	"github.com/atomix/go-local/pkg/atomix/local"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
)

func TestClient(t *testing.T) {
	coordCluster := cluster.NewCluster(
		protocolapi.ProtocolConfig{},
		cluster.WithMemberID("coordinator"),
		cluster.WithHost("localhost"),
		cluster.WithPort(5678))
	coordNode := coordinator.NewNode(coordCluster)
	err := coordNode.RegisterDriver(driverapi.DriverMeta{
		Name: "rsm",
		Proxy: proxyapi.ProxyMeta{
			Host: "localhost",
			Port: 6000,
		},
	})
	assert.NoError(t, err)
	err = coordNode.RegisterDriver(driverapi.DriverMeta{
		Name: "gossip",
		Proxy: proxyapi.ProxyMeta{
			Host: "localhost",
			Port: 6001,
		},
	})
	assert.NoError(t, err)
	err = coordNode.Start()
	assert.NoError(t, err)

	rsmConfig := protocolapi.ProtocolConfig{
		Replicas: []protocolapi.ProtocolReplica{
			{
				ID:           "rsm-1",
				NodeID:       "node-1",
				Host:         "localhost",
				ProtocolPort: 7000,
				APIPort:      7001,
			},
		},
		Partitions: []protocolapi.ProtocolPartition{
			{
				PartitionID: 1,
				Replicas:    []string{"rsm-1"},
			},
		},
	}

	rsmNode := rsmprotocol.NewNode(cluster.NewCluster(rsmConfig, cluster.WithMemberID("rsm-1")), local.NewProtocol())
	rsmcounterprotocol.RegisterService(rsmNode)
	rsmelectionprotocol.RegisterService(rsmNode)
	rsmindexedmapprotocol.RegisterService(rsmNode)
	rsmleaderprotocol.RegisterService(rsmNode)
	rsmlistprotocol.RegisterService(rsmNode)
	rsmlockprotocol.RegisterService(rsmNode)
	rsmlogprotocol.RegisterService(rsmNode)
	rsmmapprotocol.RegisterService(rsmNode)
	rsmsetprotocol.RegisterService(rsmNode)
	rsmvalueprotocol.RegisterService(rsmNode)
	err = rsmNode.Start()
	assert.NoError(t, err)

	rsmProxy := rsmproxy.NewNode(cluster.NewCluster(rsmConfig, cluster.WithMemberID("rsm-proxy-1"), cluster.WithHost("localhost"), cluster.WithPort(6000)))
	counterproxyserver.RegisterService(rsmProxy)
	electionproxyserver.RegisterService(rsmProxy)
	indexedmapproxyserver.RegisterService(rsmProxy)
	leaderproxyserver.RegisterService(rsmProxy)
	listproxyserver.RegisterService(rsmProxy)
	lockproxyserver.RegisterService(rsmProxy)
	logproxyserver.RegisterService(rsmProxy)
	mapproxyserver.RegisterService(rsmProxy)
	setproxyserver.RegisterService(rsmProxy)
	valueproxyserver.RegisterService(rsmProxy)
	rsmcounterproxy.RegisterProxy(rsmProxy)
	rsmelectionproxy.RegisterProxy(rsmProxy)
	rsmindexedmapproxy.RegisterProxy(rsmProxy)
	rsmleaderproxy.RegisterProxy(rsmProxy)
	rsmlistproxy.RegisterProxy(rsmProxy)
	rsmlockproxy.RegisterProxy(rsmProxy)
	rsmlogproxy.RegisterProxy(rsmProxy)
	rsmmapproxy.RegisterProxy(rsmProxy)
	rsmsetproxy.RegisterProxy(rsmProxy)
	rsmvalueproxy.RegisterProxy(rsmProxy)
	cachedmap.RegisterCachedMapDecorator(rsmProxy)
	romap.RegisterReadOnlyMapDecorator(rsmProxy)
	err = rsmProxy.Start()
	assert.NoError(t, err)

	gossipConfig := protocolapi.ProtocolConfig{
		Replicas: []protocolapi.ProtocolReplica{
			{
				ID:           "gossip-1",
				NodeID:       "node-1",
				Host:         "localhost",
				ProtocolPort: 8000,
				APIPort:      8001,
			},
			{
				ID:           "gossip-2",
				NodeID:       "node-2",
				Host:         "localhost",
				ProtocolPort: 8002,
				APIPort:      8003,
			},
			{
				ID:           "gossip-3",
				NodeID:       "node-3",
				Host:         "localhost",
				ProtocolPort: 8004,
				APIPort:      8005,
			},
		},
		Partitions: []protocolapi.ProtocolPartition{
			{
				PartitionID: 1,
				Replicas:    []string{"gossip-1", "gossip-2", "gossip-3"},
			},
		},
	}

	gossipNode1 := gossipprotocol.NewNode(cluster.NewCluster(gossipConfig, cluster.WithMemberID("gossip-1")), time.LogicalScheme)
	gossipcounterprotocol.RegisterService(gossipNode1)
	gossipmapprotocol.RegisterService(gossipNode1)
	gossipsetprotocol.RegisterService(gossipNode1)
	gossipvalueprotocol.RegisterService(gossipNode1)
	gossipcounterprotocol.RegisterServer(gossipNode1)
	gossipmapprotocol.RegisterServer(gossipNode1)
	gossipsetprotocol.RegisterServer(gossipNode1)
	gossipvalueprotocol.RegisterServer(gossipNode1)
	err = gossipNode1.Start()
	assert.NoError(t, err)

	gossipNode2 := gossipprotocol.NewNode(cluster.NewCluster(gossipConfig, cluster.WithMemberID("gossip-2")), time.LogicalScheme)
	gossipcounterprotocol.RegisterService(gossipNode2)
	gossipmapprotocol.RegisterService(gossipNode2)
	gossipsetprotocol.RegisterService(gossipNode2)
	gossipvalueprotocol.RegisterService(gossipNode2)
	gossipcounterprotocol.RegisterServer(gossipNode2)
	gossipmapprotocol.RegisterServer(gossipNode2)
	gossipsetprotocol.RegisterServer(gossipNode2)
	gossipvalueprotocol.RegisterServer(gossipNode2)
	err = gossipNode2.Start()
	assert.NoError(t, err)

	gossipNode3 := gossipprotocol.NewNode(cluster.NewCluster(gossipConfig, cluster.WithMemberID("gossip-3")), time.LogicalScheme)
	gossipcounterprotocol.RegisterService(gossipNode3)
	gossipmapprotocol.RegisterService(gossipNode3)
	gossipsetprotocol.RegisterService(gossipNode3)
	gossipvalueprotocol.RegisterService(gossipNode3)
	gossipcounterprotocol.RegisterServer(gossipNode3)
	gossipmapprotocol.RegisterServer(gossipNode3)
	gossipsetprotocol.RegisterServer(gossipNode3)
	gossipvalueprotocol.RegisterServer(gossipNode3)
	err = gossipNode3.Start()
	assert.NoError(t, err)

	gossipProxy := gossipproxy.NewNode(cluster.NewCluster(gossipConfig, cluster.WithMemberID("gossip-proxy-1"), cluster.WithHost("localhost"), cluster.WithPort(6001)), time.LogicalScheme)
	counterproxyserver.RegisterService(gossipProxy)
	mapproxyserver.RegisterService(gossipProxy)
	setproxyserver.RegisterService(gossipProxy)
	valueproxyserver.RegisterService(gossipProxy)
	gossipcounterproxy.RegisterProxy(gossipProxy)
	gossipmapproxy.RegisterProxy(gossipProxy)
	gossipsetproxy.RegisterProxy(gossipProxy)
	gossipvalueproxy.RegisterProxy(gossipProxy)
	cachedmap.RegisterCachedMapDecorator(gossipProxy)
	romap.RegisterReadOnlyMapDecorator(gossipProxy)
	err = gossipProxy.Start()
	assert.NoError(t, err)

	coordConn, err := grpc.Dial("localhost:5678", grpc.WithInsecure())
	assert.NoError(t, err)
	primitiveManagementClient := primitiveapi.NewPrimitiveManagementServiceClient(coordConn)

	rsmMapMeta := primitiveapi.PrimitiveMeta{
		Name:   "rsm-map",
		Type:   _map.Type.String(),
		Driver: "rsm",
	}
	_, err = primitiveManagementClient.AddPrimitive(context.TODO(), &primitiveapi.AddPrimitiveRequest{Primitive: rsmMapMeta})
	assert.NoError(t, err)

	rsmCachedMapMeta := primitiveapi.PrimitiveMeta{
		Name:   "rsm-cached-map",
		Type:   _map.Type.String(),
		Driver: "rsm",
		Cached: true,
	}
	_, err = primitiveManagementClient.AddPrimitive(context.TODO(), &primitiveapi.AddPrimitiveRequest{Primitive: rsmCachedMapMeta})
	assert.NoError(t, err)

	rsmReadOnlyMapMeta := primitiveapi.PrimitiveMeta{
		Name:   "rsm-read-only-map",
		Type:   _map.Type.String(),
		Driver: "rsm",
		Cached: true,
	}
	_, err = primitiveManagementClient.AddPrimitive(context.TODO(), &primitiveapi.AddPrimitiveRequest{Primitive: rsmReadOnlyMapMeta})
	assert.NoError(t, err)

	gossipMapMeta := primitiveapi.PrimitiveMeta{
		Name:   "gossip-map",
		Type:   _map.Type.String(),
		Driver: "gossip",
	}
	_, err = primitiveManagementClient.AddPrimitive(context.TODO(), &primitiveapi.AddPrimitiveRequest{Primitive: gossipMapMeta})
	assert.NoError(t, err)

	gossipCachedMapMeta := primitiveapi.PrimitiveMeta{
		Name:   "gossip-cached-map",
		Type:   _map.Type.String(),
		Driver: "gossip",
		Cached: true,
	}
	_, err = primitiveManagementClient.AddPrimitive(context.TODO(), &primitiveapi.AddPrimitiveRequest{Primitive: gossipCachedMapMeta})
	assert.NoError(t, err)

	gossipReadOnlyMapMeta := primitiveapi.PrimitiveMeta{
		Name:   "gossip-read-only-map",
		Type:   _map.Type.String(),
		Driver: "gossip",
		Cached: true,
	}
	_, err = primitiveManagementClient.AddPrimitive(context.TODO(), &primitiveapi.AddPrimitiveRequest{Primitive: gossipReadOnlyMapMeta})
	assert.NoError(t, err)

	err = coordConn.Close()
	assert.NoError(t, err)

	client := NewClient()

	rsmMap, err := client.GetMap(context.TODO(), "rsm-map")
	assert.NoError(t, err)
	assert.NotNil(t, rsmMap)

	i, err := rsmMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, i)

	rsmCachedMap, err := client.GetMap(context.TODO(), "rsm-cached-map")
	assert.NoError(t, err)
	assert.NotNil(t, rsmCachedMap)

	i, err = rsmCachedMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, i)

	rsmReadOnlyMap, err := client.GetMap(context.TODO(), "rsm-read-only-map")
	assert.NoError(t, err)
	assert.NotNil(t, rsmReadOnlyMap)

	i, err = rsmReadOnlyMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, i)

	gossipMap, err := client.GetMap(context.TODO(), "gossip-map")
	assert.NoError(t, err)
	assert.NotNil(t, gossipMap)

	i, err = gossipMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, i)

	gossipCachedMap, err := client.GetMap(context.TODO(), "gossip-cached-map")
	assert.NoError(t, err)
	assert.NotNil(t, gossipCachedMap)

	i, err = gossipCachedMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, i)

	gossipReadOnlyMap, err := client.GetMap(context.TODO(), "gossip-read-only-map")
	assert.NoError(t, err)
	assert.NotNil(t, gossipReadOnlyMap)

	i, err = gossipReadOnlyMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, i)
}
