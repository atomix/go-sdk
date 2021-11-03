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
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver/v1"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol/v1"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-sdk-go/pkg/cluster"
	"github.com/atomix/atomix-sdk-go/pkg/driver"
	"github.com/atomix/atomix-sdk-go/pkg/driver/env"
	"github.com/atomix/atomix-sdk-go/pkg/driver/proxy"
	"github.com/atomix/atomix-sdk-go/pkg/driver/proxy/gossip"
	gossipcounterproxy "github.com/atomix/atomix-sdk-go/pkg/driver/proxy/gossip/counter"
	gossipmapproxy "github.com/atomix/atomix-sdk-go/pkg/driver/proxy/gossip/map"
	gossipsetproxy "github.com/atomix/atomix-sdk-go/pkg/driver/proxy/gossip/set"
	gossipvalueproxy "github.com/atomix/atomix-sdk-go/pkg/driver/proxy/gossip/value"
	"github.com/atomix/atomix-sdk-go/pkg/errors"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
)

func newClient(network cluster.Network, clientID string, config protocolapi.ProtocolConfig, options gossipOptions) *gossipClient {
	return &gossipClient{
		network: network,
		id:      clientID,
		config:  config,
		options: options,
	}
}

type gossipClient struct {
	network cluster.Network
	id      string
	config  protocolapi.ProtocolConfig
	options gossipOptions
	driver  *driver.Driver
	conn    *grpc.ClientConn
}

func (c *gossipClient) Start(driverPort, agentPort int) error {
	protocolFunc := func(c cluster.Cluster, env env.DriverEnv) proxy.Protocol {
		p := gossip.NewProtocol(c, env)
		gossipcounterproxy.Register(p)
		gossipmapproxy.Register(p)
		gossipsetproxy.Register(p)
		gossipvalueproxy.Register(p)
		return p
	}

	cluster := cluster.NewCluster(
		c.network,
		protocolapi.ProtocolConfig{},
		cluster.WithMemberID("gossip-driver"),
		cluster.WithPort(driverPort))
	c.driver = driver.NewDriver(cluster, protocolFunc, driver.WithNamespace("test"))
	err := c.driver.Start()
	if err != nil {
		return err
	}

	driverConn, err := grpc.Dial(fmt.Sprintf(":%d", driverPort), grpc.WithInsecure(), grpc.WithContextDialer(c.network.Connect))
	if err != nil {
		return err
	}
	defer driverConn.Close()
	driverClient := driverapi.NewDriverClient(driverConn)

	agentID := driverapi.AgentId{
		ProtocolId: protocolapi.ProtocolId{
			Namespace: "test",
			Name:      "rsm",
		},
	}
	agentAddress := driverapi.AgentAddress{
		Host: "localhost",
		Port: int32(agentPort),
	}
	agentConfig := driverapi.AgentConfig{
		Protocol: c.config,
	}

	_, err = driverClient.StartAgent(context.TODO(), &driverapi.StartAgentRequest{AgentID: agentID, Address: agentAddress, Config: agentConfig})
	if err != nil {
		return err
	}

	c.conn, err = grpc.Dial(fmt.Sprintf(":%d", agentPort), grpc.WithInsecure(), grpc.WithContextDialer(c.network.Connect))
	if err != nil {
		return err
	}
	return nil
}

func (c *gossipClient) Connect(ctx context.Context, primitive primitive.Type, name string) (*grpc.ClientConn, error) {
	marshaler := jsonpb.Marshaler{}
	bytes, err := marshaler.MarshalToString(&c.options.GossipConfig)
	if err != nil {
		return nil, err
	}

	agentClient := driverapi.NewAgentClient(c.conn)
	request := &driverapi.CreateProxyRequest{
		ProxyID: driverapi.ProxyId{Type: primitive.String(), Name: name},
		Options: driverapi.ProxyOptions{
			Read:   true,
			Write:  true,
			Config: []byte(bytes),
		},
	}
	_, err = agentClient.CreateProxy(ctx, request)
	if err != nil && !errors.IsAlreadyExists(errors.From(err)) {
		return nil, err
	}
	return c.conn, nil
}

func (c *gossipClient) Stop() error {
	if c.conn != nil {
		c.conn.Close()
	}
	if c.driver != nil {
		err := c.driver.Stop()
		if err != nil {
			return err
		}
	}
	return nil
}
