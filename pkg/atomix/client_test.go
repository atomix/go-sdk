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
	brokerapi "github.com/atomix/api/go/atomix/management/broker"
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	_map "github.com/atomix/go-client/pkg/atomix/map"
	"github.com/atomix/go-framework/pkg/atomix/broker"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	gossipdriver "github.com/atomix/go-framework/pkg/atomix/driver/protocol/gossip"
	rsmdriver "github.com/atomix/go-framework/pkg/atomix/driver/protocol/rsm"
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
	atime "github.com/atomix/go-framework/pkg/atomix/time"
	"github.com/atomix/go-local/pkg/atomix/local"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	brokerNode := broker.NewNode()
	err := brokerNode.Start()
	assert.NoError(t, err)

	rsmConfig := protocolapi.ProtocolConfig{
		Replicas: []protocolapi.ProtocolReplica{
			{
				ID:      "rsm-1",
				NodeID:  "node-1",
				Host:    "localhost",
				APIPort: 7001,
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

	rsmCluster := cluster.NewCluster(rsmConfig, cluster.WithMemberID("rsm"), cluster.WithPort(55680))
	rsmDriver := rsmdriver.NewNode(rsmCluster)
	rsmdriver.RegisterCounterProxy(rsmDriver)
	rsmdriver.RegisterElectionProxy(rsmDriver)
	rsmdriver.RegisterIndexedMapProxy(rsmDriver)
	rsmdriver.RegisterLockProxy(rsmDriver)
	rsmdriver.RegisterLogProxy(rsmDriver)
	rsmdriver.RegisterLeaderLatchProxy(rsmDriver)
	rsmdriver.RegisterListProxy(rsmDriver)
	rsmdriver.RegisterMapProxy(rsmDriver)
	rsmdriver.RegisterSetProxy(rsmDriver)
	rsmdriver.RegisterValueProxy(rsmDriver)
	err = rsmDriver.Start()
	assert.NoError(t, err)

	gossipConfig := protocolapi.ProtocolConfig{
		Replicas: []protocolapi.ProtocolReplica{
			{
				ID:      "gossip-1",
				NodeID:  "node-1",
				Host:    "localhost",
				APIPort: 8000,
			},
			{
				ID:      "gossip-2",
				NodeID:  "node-2",
				Host:    "localhost",
				APIPort: 8001,
			},
			{
				ID:      "gossip-3",
				NodeID:  "node-3",
				Host:    "localhost",
				APIPort: 8002,
			},
		},
		Partitions: []protocolapi.ProtocolPartition{
			{
				PartitionID: 1,
				Replicas:    []string{"gossip-1", "gossip-2", "gossip-3"},
			},
		},
	}

	gossipNode1 := gossipprotocol.NewNode(cluster.NewCluster(gossipConfig, cluster.WithMemberID("gossip-1")), atime.LogicalScheme)
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

	gossipNode2 := gossipprotocol.NewNode(cluster.NewCluster(gossipConfig, cluster.WithMemberID("gossip-2")), atime.LogicalScheme)
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

	gossipNode3 := gossipprotocol.NewNode(cluster.NewCluster(gossipConfig, cluster.WithMemberID("gossip-3")), atime.LogicalScheme)
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

	gossipCluster := cluster.NewCluster(gossipConfig, cluster.WithMemberID("gossip"), cluster.WithPort(55681))
	gossipDriver := gossipdriver.NewNode(gossipCluster, atime.LogicalScheme)
	gossipdriver.RegisterCounterProxy(gossipDriver)
	gossipdriver.RegisterMapProxy(gossipDriver)
	gossipdriver.RegisterSetProxy(gossipDriver)
	gossipdriver.RegisterValueProxy(gossipDriver)
	err = gossipDriver.Start()
	assert.NoError(t, err)

	brokerConn, err := grpc.Dial("localhost:5151", grpc.WithInsecure())
	assert.NoError(t, err)
	defer brokerConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	driverManagementClient := brokerapi.NewDriverManagementServiceClient(brokerConn)

	_, err = driverManagementClient.AddDriver(ctx, &brokerapi.AddDriverRequest{
		Driver: brokerapi.DriverConfig{
			ID: brokerapi.DriverId{
				Type:      "rsm",
				Namespace: "test",
				Name:      "rsm",
			},
			Host: defaultHost,
			Port: 55680,
		},
	})
	assert.NoError(t, err)

	_, err = driverManagementClient.AddDriver(ctx, &brokerapi.AddDriverRequest{
		Driver: brokerapi.DriverConfig{
			ID: brokerapi.DriverId{
				Type:      "gossip",
				Namespace: "test",
				Name:      "gossip",
			},
			Host: defaultHost,
			Port: 55681,
		},
	})
	assert.NoError(t, err)

	primitiveManagementClient := brokerapi.NewPrimitiveManagementServiceClient(brokerConn)

	rsmMapConfig := brokerapi.PrimitiveConfig{
		ID: brokerapi.PrimitiveId{
			Type:      _map.Type.String(),
			Namespace: "test",
			Name:      "rsm-map",
		},
		Driver: brokerapi.DriverId{
			Type:      "rsm",
			Namespace: "test",
			Name:      "rsm",
		},
		Proxy: brokerapi.ProxyConfig{
			Read:  true,
			Write: true,
		},
	}
	_, err = primitiveManagementClient.AddPrimitive(context.TODO(), &brokerapi.AddPrimitiveRequest{Primitive: rsmMapConfig})
	assert.NoError(t, err)

	rsmReadOnlyMapConfig := brokerapi.PrimitiveConfig{
		ID: brokerapi.PrimitiveId{
			Type:      _map.Type.String(),
			Namespace: "test",
			Name:      "rsm-read-only-map",
		},
		Driver: brokerapi.DriverId{
			Type:      "rsm",
			Namespace: "test",
			Name:      "rsm",
		},
		Proxy: brokerapi.ProxyConfig{
			Read:  true,
			Write: false,
		},
	}
	_, err = primitiveManagementClient.AddPrimitive(context.TODO(), &brokerapi.AddPrimitiveRequest{Primitive: rsmReadOnlyMapConfig})
	assert.NoError(t, err)

	gossipMapConfig := brokerapi.PrimitiveConfig{
		ID: brokerapi.PrimitiveId{
			Type:      _map.Type.String(),
			Namespace: "test",
			Name:      "gossip-map",
		},
		Driver: brokerapi.DriverId{
			Type:      "gossip",
			Namespace: "test",
			Name:      "gossip",
		},
		Proxy: brokerapi.ProxyConfig{
			Read:  true,
			Write: true,
		},
	}
	_, err = primitiveManagementClient.AddPrimitive(context.TODO(), &brokerapi.AddPrimitiveRequest{Primitive: gossipMapConfig})
	assert.NoError(t, err)

	gossipReadOnlyMapConfig := brokerapi.PrimitiveConfig{
		ID: brokerapi.PrimitiveId{
			Type:      _map.Type.String(),
			Namespace: "test",
			Name:      "gossip-read-only-map",
		},
		Driver: brokerapi.DriverId{
			Type:      "gossip",
			Namespace: "test",
			Name:      "gossip",
		},
		Proxy: brokerapi.ProxyConfig{
			Read:  true,
			Write: false,
		},
	}
	_, err = primitiveManagementClient.AddPrimitive(context.TODO(), &brokerapi.AddPrimitiveRequest{Primitive: gossipReadOnlyMapConfig})
	assert.NoError(t, err)

	err = brokerConn.Close()
	assert.NoError(t, err)

	client := NewClient().Namespace("test")

	rsmMap, err := client.GetMap(context.TODO(), "rsm-map")
	assert.NoError(t, err)
	assert.NotNil(t, rsmMap)

	i, err := rsmMap.Len(context.TODO())
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

	gossipReadOnlyMap, err := client.GetMap(context.TODO(), "gossip-read-only-map")
	assert.NoError(t, err)
	assert.NotNil(t, gossipReadOnlyMap)

	i, err = gossipReadOnlyMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, i)
}
