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
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip"
	gossipcounterproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip/counter"
	gossipmapproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip/map"
	gossipsetproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip/set"
	gossipvalueproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"google.golang.org/grpc"
)

func newClient(clientID string, config protocolapi.ProtocolConfig) *gossipClient {
	return &gossipClient{
		id:     clientID,
		config: config,
	}
}

type gossipClient struct {
	id     string
	config protocolapi.ProtocolConfig
	driver *driver.Driver
	conn   *grpc.ClientConn
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

	c.driver = driver.NewDriver(protocolFunc, driver.WithNamespace("test"), driver.WithDriverID("driver"), driver.WithPort(driverPort))
	err := c.driver.Start()
	if err != nil {
		return err
	}

	driverConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", driverPort), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer driverConn.Close()
	driverClient := driverapi.NewDriverClient(driverConn)

	agentID := driverapi.AgentId{
		Namespace: "test",
		Name:      "rsm",
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

	c.conn, err = grpc.Dial(fmt.Sprintf("localhost:%d", agentPort), grpc.WithInsecure())
	if err != nil {
		return err
	}
	return nil
}

func (c *gossipClient) Connect(ctx context.Context, primitive primitive.Type, name string) (*grpc.ClientConn, error) {
	agentClient := driverapi.NewAgentClient(c.conn)
	proxyOptions := driverapi.ProxyOptions{
		Read:  true,
		Write: true,
	}
	primitiveID := primitiveapi.PrimitiveId{Type: primitive.String(), Namespace: "test", Name: name}
	_, err := agentClient.CreateProxy(ctx, &driverapi.CreateProxyRequest{ProxyID: driverapi.ProxyId{PrimitiveId: primitiveID}, Options: proxyOptions})
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
