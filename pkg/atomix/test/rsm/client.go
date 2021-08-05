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

package rsm

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
	rsmdriver "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm"
	rsmcounterproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/counter"
	rsmelectionproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/election"
	rsmindexedmapproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/indexedmap"
	rsmlistproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/list"
	rsmlockproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/lock"
	rsmmapproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/map"
	rsmsetproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/set"
	rsmvalueproxy "github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/rsm/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"google.golang.org/grpc"
)

func newClient(network cluster.Network, clientID string, config protocolapi.ProtocolConfig) *testClient {
	return &testClient{
		network: network,
		id:      clientID,
		config:  config,
	}
}

type testClient struct {
	network cluster.Network
	id      string
	config  protocolapi.ProtocolConfig
	driver  *driver.Driver
	conn    *grpc.ClientConn
}

func (c *testClient) Start(driverPort, agentPort int) error {
	protocolFunc := func(rsmCluster cluster.Cluster, driverEnv env.DriverEnv) proxy.Protocol {
		protocol := rsmdriver.NewProtocol(rsmCluster, driverEnv)
		rsmcounterproxy.Register(protocol)
		rsmelectionproxy.Register(protocol)
		rsmindexedmapproxy.Register(protocol)
		rsmlistproxy.Register(protocol)
		rsmlockproxy.Register(protocol)
		rsmmapproxy.Register(protocol)
		rsmsetproxy.Register(protocol)
		rsmvalueproxy.Register(protocol)
		return protocol
	}

	cluster := cluster.NewCluster(
		c.network,
		protocolapi.ProtocolConfig{},
		cluster.WithMemberID("rsm-driver"),
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

	c.conn, err = grpc.Dial(fmt.Sprintf(":%d", agentPort), grpc.WithInsecure(), grpc.WithContextDialer(c.network.Connect))
	if err != nil {
		return err
	}
	return nil
}

func (c *testClient) Connect(ctx context.Context, primitive primitive.Type, name string) (*grpc.ClientConn, error) {
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

func (c *testClient) Stop() error {
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
