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

package gossip

import (
	"context"
	"fmt"
	brokerapi "github.com/atomix/api/go/atomix/management/broker"
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/broker"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/driver"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy"
	gossipdriver "github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip"
	gossipcounterproxy "github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip/counter"
	gossipmapproxy "github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip/map"
	gossipsetproxy "github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip/set"
	gossipvalueproxy "github.com/atomix/go-framework/pkg/atomix/driver/proxy/gossip/value"
	atime "github.com/atomix/go-framework/pkg/atomix/time"
	"google.golang.org/grpc"
)

const defaultHost = "localhost"

type Node struct {
	cluster    *Cluster
	brokerPort int
	driverPort int
	agentPort  int
	broker     *broker.Broker
	driver     *driver.Driver
}

func (n *Node) AddPrimitive(t primitive.Type, name string) error {
	agentConn, err := grpc.Dial(fmt.Sprintf("%s:%d", defaultHost, n.agentPort), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer agentConn.Close()
	agentClient := driverapi.NewAgentClient(agentConn)
	primitiveAddress := brokerapi.PrimitiveAddress{
		Host: defaultHost,
		Port: int32(n.agentPort),
	}

	primitiveID := primitiveapi.PrimitiveId{
		Type:      t.String(),
		Namespace: "test",
		Name:      name,
	}

	primitiveOptions := driverapi.ProxyOptions{
		Read:  true,
		Write: true,
	}
	_, err = agentClient.CreateProxy(context.TODO(), &driverapi.CreateProxyRequest{ProxyID: driverapi.ProxyId{PrimitiveId: primitiveID}, Options: primitiveOptions})
	if err != nil {
		return err
	}

	brokerConn, err := grpc.Dial(fmt.Sprintf("%s:%d", defaultHost, n.brokerPort), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer brokerConn.Close()
	brokerClient := brokerapi.NewBrokerClient(brokerConn)
	_, err = brokerClient.RegisterPrimitive(context.TODO(), &brokerapi.RegisterPrimitiveRequest{PrimitiveID: brokerapi.PrimitiveId{PrimitiveId: primitiveID}, Address: primitiveAddress})
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) Start() error {
	n.broker = broker.NewBroker(broker.WithPort(n.brokerPort))
	if err := n.broker.Start(); err != nil {
		return err
	}

	protocol := func(gossipCluster cluster.Cluster) proxy.Protocol {
		gossipProxyProtocol := gossipdriver.NewProtocol(gossipCluster, atime.LogicalScheme)
		gossipcounterproxy.Register(gossipProxyProtocol)
		gossipmapproxy.Register(gossipProxyProtocol)
		gossipsetproxy.Register(gossipProxyProtocol)
		gossipvalueproxy.Register(gossipProxyProtocol)
		return gossipProxyProtocol
	}
	n.driver = driver.NewDriver(protocol, driver.WithDriverID("gossip"), driver.WithPort(n.driverPort))
	if err := n.driver.Start(); err != nil {
		return err
	}

	driverConn, err := grpc.Dial(fmt.Sprintf("%s:%d", defaultHost, n.driverPort), grpc.WithInsecure())
	if err != nil {
		return err
	}
	agentID := driverapi.AgentId{
		Namespace: "test",
		Name:      "gossip",
	}
	agentAddress := driverapi.AgentAddress{
		Host: defaultHost,
		Port: int32(n.agentPort),
	}
	agentConfig := driverapi.AgentConfig{
		Protocol: n.cluster.config,
	}

	driverClient := driverapi.NewDriverClient(driverConn)
	_, err = driverClient.StartAgent(context.TODO(), &driverapi.StartAgentRequest{AgentID: agentID, Address: agentAddress, Config: agentConfig})
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) Stop() error {
	panic("implement me")
}
