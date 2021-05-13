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
	brokerapi "github.com/atomix/atomix-api/go/atomix/management/broker"
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	_map "github.com/atomix/atomix-go-client/pkg/atomix/map"
	"github.com/atomix/atomix-go-framework/pkg/atomix/broker"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy"
	gossipdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip"
	gossipcounterproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip/counter"
	gossipmapproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip/map"
	gossipsetproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip/set"
	gossipvalueproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip/value"
	rsmdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	rsmcounterproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/counter"
	rsmelectionproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/election"
	rsmindexedmapproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/indexedmap"
	rsmleaderproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/leader"
	rsmlistproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/list"
	rsmlockproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/lock"
	rsmlogproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/log"
	rsmmapproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/map"
	rsmsetproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/set"
	rsmvalueproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	gossipprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip"
	gossipcounterprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/counter"
	gossipmapprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/map"
	gossipsetprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/set"
	gossipvalueprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/value"
	rsmprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	rsmcounterprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/counter"
	rsmelectionprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/election"
	rsmindexedmapprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/indexedmap"
	rsmleaderprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/leader"
	rsmlistprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/list"
	rsmlockprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/lock"
	rsmlogprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/log"
	rsmmapprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/map"
	rsmsetprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/set"
	rsmvalueprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/value"
	atime "github.com/atomix/atomix-go-framework/pkg/atomix/time"
	"github.com/atomix/atomix-go-local/pkg/atomix/local"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
)

func TestRSMMap(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	/*
		node := test.NewNode("foo")
		node.NewBroker().Start()

		test.Storage().RSM().SetSomething(...)
		test.Storage().Gossip().SetSomethingElse(...)

		test.Node("foo").Drivers().RSM().

		node := test.NewNode("foo")
		rsmDriver := node.NewDriver(protocol.RSM)
		gossipDriver := node.NewDriver(protocol.Gossip)
	*/

	brokerNode := broker.NewBroker(broker.WithPort(5678), broker.WithNamespace("test"))
	err := brokerNode.Start()
	assert.NoError(t, err)

	brokerConn, err := grpc.Dial("localhost:5678", grpc.WithInsecure())
	assert.NoError(t, err)
	defer brokerConn.Close()
	brokerClient := brokerapi.NewBrokerClient(brokerConn)

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

	rsmProtocolFunc := func(rsmCluster cluster.Cluster, driverEnv env.DriverEnv) proxy.Protocol {
		rsmProxyProtocol := rsmdriver.NewProtocol(rsmCluster, driverEnv)
		rsmcounterproxy.Register(rsmProxyProtocol)
		rsmelectionproxy.Register(rsmProxyProtocol)
		rsmindexedmapproxy.Register(rsmProxyProtocol)
		rsmleaderproxy.Register(rsmProxyProtocol)
		rsmlistproxy.Register(rsmProxyProtocol)
		rsmlockproxy.Register(rsmProxyProtocol)
		rsmlogproxy.Register(rsmProxyProtocol)
		rsmmapproxy.Register(rsmProxyProtocol)
		rsmsetproxy.Register(rsmProxyProtocol)
		rsmvalueproxy.Register(rsmProxyProtocol)
		return rsmProxyProtocol
	}

	rsmDriver := driver.NewDriver(rsmProtocolFunc, driver.WithNamespace("test"), driver.WithDriverID("rsm"), driver.WithPort(5252))
	err = rsmDriver.Start()
	assert.NoError(t, err)

	rsmDriverConn, err := grpc.Dial("localhost:5252", grpc.WithInsecure())
	assert.NoError(t, err)
	defer rsmDriverConn.Close()
	rsmDriverClient := driverapi.NewDriverClient(rsmDriverConn)

	rsmAgentID := driverapi.AgentId{
		Namespace: "test",
		Name:      "rsm",
	}
	rsmAgentAddress := driverapi.AgentAddress{
		Host: defaultHost,
		Port: 55680,
	}
	rsmAgentConfig := driverapi.AgentConfig{
		Protocol: rsmConfig,
	}

	_, err = rsmDriverClient.StartAgent(context.TODO(), &driverapi.StartAgentRequest{AgentID: rsmAgentID, Address: rsmAgentAddress, Config: rsmAgentConfig})
	assert.NoError(t, err)

	rsmAgentConn, err := grpc.Dial("localhost:55680", grpc.WithInsecure())
	assert.NoError(t, err)
	defer rsmAgentConn.Close()
	rsmAgentClient := driverapi.NewAgentClient(rsmAgentConn)
	rsmPrimitiveAddress := brokerapi.PrimitiveAddress{
		Host: defaultHost,
		Port: 55680,
	}

	rsmMapID := primitiveapi.PrimitiveId{
		Type:      _map.Type.String(),
		Namespace: "test",
		Name:      "rsm-map",
	}
	_, err = brokerClient.RegisterPrimitive(context.TODO(), &brokerapi.RegisterPrimitiveRequest{PrimitiveID: brokerapi.PrimitiveId{rsmMapID}, Address: rsmPrimitiveAddress})
	assert.NoError(t, err)

	rsmMapOptions := driverapi.ProxyOptions{
		Read:  true,
		Write: true,
	}
	_, err = rsmAgentClient.CreateProxy(context.TODO(), &driverapi.CreateProxyRequest{ProxyID: driverapi.ProxyId{rsmMapID}, Options: rsmMapOptions})

	rsmReadOnlyMapID := primitiveapi.PrimitiveId{
		Type:      _map.Type.String(),
		Namespace: "test",
		Name:      "rsm-read-only-map",
	}
	_, err = brokerClient.RegisterPrimitive(context.TODO(), &brokerapi.RegisterPrimitiveRequest{PrimitiveID: brokerapi.PrimitiveId{rsmReadOnlyMapID}, Address: rsmPrimitiveAddress})
	assert.NoError(t, err)

	rsmReadOnlyMapOptions := driverapi.ProxyOptions{
		Read:  true,
		Write: false,
	}
	_, err = rsmAgentClient.CreateProxy(context.TODO(), &driverapi.CreateProxyRequest{ProxyID: driverapi.ProxyId{rsmReadOnlyMapID}, Options: rsmReadOnlyMapOptions})

	err = brokerConn.Close()
	assert.NoError(t, err)

	client := NewClient()

	rsmMap, err := client.GetMap(context.TODO(), "rsm-map")
	assert.NoError(t, err)
	assert.NotNil(t, rsmMap)

	_, err = rsmMap.Put(context.TODO(), "foo", []byte("bar"))
	assert.NoError(t, err)

	entry, err := rsmMap.Get(context.TODO(), "foo")
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "bar", string(entry.Value))

	i, err := rsmMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 1, i)

	rsmReadOnlyMap, err := client.GetMap(context.TODO(), "rsm-read-only-map")
	assert.NoError(t, err)
	assert.NotNil(t, rsmReadOnlyMap)

	i, err = rsmReadOnlyMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, i)
}

func TestGossipMap(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	brokerNode := broker.NewBroker(broker.WithPort(5678), broker.WithNamespace("test"))
	err := brokerNode.Start()
	assert.NoError(t, err)

	brokerConn, err := grpc.Dial("localhost:5678", grpc.WithInsecure())
	assert.NoError(t, err)
	defer brokerConn.Close()
	brokerClient := brokerapi.NewBrokerClient(brokerConn)

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

	gossipProtocolFunc := func(gossipCluster cluster.Cluster, driverEnv env.DriverEnv) proxy.Protocol {
		gossipProxyProtocol := gossipdriver.NewProtocol(gossipCluster, driverEnv, atime.LogicalScheme)
		gossipcounterproxy.Register(gossipProxyProtocol)
		gossipmapproxy.Register(gossipProxyProtocol)
		gossipsetproxy.Register(gossipProxyProtocol)
		gossipvalueproxy.Register(gossipProxyProtocol)
		return gossipProxyProtocol
	}

	gossipDriver := driver.NewDriver(gossipProtocolFunc, driver.WithNamespace("test"), driver.WithDriverID("gossip"), driver.WithPort(5353))
	err = gossipDriver.Start()
	assert.NoError(t, err)

	gossipDriverConn, err := grpc.Dial("localhost:5353", grpc.WithInsecure())
	assert.NoError(t, err)
	defer gossipDriverConn.Close()
	gossipDriverClient := driverapi.NewDriverClient(gossipDriverConn)

	gossipAgentID := driverapi.AgentId{
		Namespace: "test",
		Name:      "gossip",
	}
	gossipAgentAddress := driverapi.AgentAddress{
		Host: defaultHost,
		Port: 55681,
	}
	gossipAgentConfig := driverapi.AgentConfig{
		Protocol: gossipConfig,
	}

	_, err = gossipDriverClient.StartAgent(context.TODO(), &driverapi.StartAgentRequest{AgentID: gossipAgentID, Address: gossipAgentAddress, Config: gossipAgentConfig})
	assert.NoError(t, err)

	gossipAgentConn, err := grpc.Dial("localhost:55681", grpc.WithInsecure())
	assert.NoError(t, err)
	defer gossipAgentConn.Close()
	gossipAgentClient := driverapi.NewAgentClient(gossipAgentConn)
	gossipPrimitiveAddress := brokerapi.PrimitiveAddress{
		Host: defaultHost,
		Port: 55681,
	}

	gossipMapID := primitiveapi.PrimitiveId{
		Type:      _map.Type.String(),
		Namespace: "test",
		Name:      "gossip-map",
	}
	_, err = brokerClient.RegisterPrimitive(context.TODO(), &brokerapi.RegisterPrimitiveRequest{PrimitiveID: brokerapi.PrimitiveId{gossipMapID}, Address: gossipPrimitiveAddress})
	assert.NoError(t, err)

	gossipMapOptions := driverapi.ProxyOptions{
		Read:  true,
		Write: true,
	}
	_, err = gossipAgentClient.CreateProxy(context.TODO(), &driverapi.CreateProxyRequest{ProxyID: driverapi.ProxyId{gossipMapID}, Options: gossipMapOptions})

	gossipReadOnlyMapID := primitiveapi.PrimitiveId{
		Type:      _map.Type.String(),
		Namespace: "test",
		Name:      "gossip-read-only-map",
	}
	_, err = brokerClient.RegisterPrimitive(context.TODO(), &brokerapi.RegisterPrimitiveRequest{PrimitiveID: brokerapi.PrimitiveId{gossipReadOnlyMapID}, Address: gossipPrimitiveAddress})
	assert.NoError(t, err)

	gossipReadOnlyMapOptions := driverapi.ProxyOptions{
		Read:  true,
		Write: false,
	}
	_, err = gossipAgentClient.CreateProxy(context.TODO(), &driverapi.CreateProxyRequest{ProxyID: driverapi.ProxyId{gossipReadOnlyMapID}, Options: gossipReadOnlyMapOptions})

	err = brokerConn.Close()
	assert.NoError(t, err)

	client := NewClient()

	gossipMap, err := client.GetMap(context.TODO(), "gossip-map")
	assert.NoError(t, err)
	assert.NotNil(t, gossipMap)

	_, err = gossipMap.Put(context.TODO(), "foo", []byte("bar"))
	assert.NoError(t, err)

	entry, err := gossipMap.Get(context.TODO(), "foo")
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "bar", string(entry.Value))

	i, err := gossipMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 1, i)

	gossipReadOnlyMap, err := client.GetMap(context.TODO(), "gossip-read-only-map")
	assert.NoError(t, err)
	assert.NotNil(t, gossipReadOnlyMap)

	i, err = gossipReadOnlyMap.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, i)
}
