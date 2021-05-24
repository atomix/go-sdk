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

package test

import (
	"context"
	"fmt"
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-client/pkg/atomix/counter"
	"github.com/atomix/atomix-go-client/pkg/atomix/election"
	"github.com/atomix/atomix-go-client/pkg/atomix/indexedmap"
	"github.com/atomix/atomix-go-client/pkg/atomix/list"
	"github.com/atomix/atomix-go-client/pkg/atomix/lock"
	_map "github.com/atomix/atomix-go-client/pkg/atomix/map"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/set"
	"github.com/atomix/atomix-go-client/pkg/atomix/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy"
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
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"google.golang.org/grpc"
)

func newClient(clientID string, config protocolapi.ProtocolConfig) *testClient {
	return &testClient{
		id:     clientID,
		config: config,
	}
}

type testClient struct {
	id     string
	config protocolapi.ProtocolConfig
	driver *driver.Driver
	conn   *grpc.ClientConn
}

func (c *testClient) connect(driverPort, agentPort int) error {
	protocolFunc := func(rsmCluster cluster.Cluster, driverEnv env.DriverEnv) proxy.Protocol {
		protocol := rsmdriver.NewProtocol(rsmCluster, driverEnv)
		rsmcounterproxy.Register(protocol)
		rsmelectionproxy.Register(protocol)
		rsmindexedmapproxy.Register(protocol)
		rsmleaderproxy.Register(protocol)
		rsmlistproxy.Register(protocol)
		rsmlockproxy.Register(protocol)
		rsmlogproxy.Register(protocol)
		rsmmapproxy.Register(protocol)
		rsmsetproxy.Register(protocol)
		rsmvalueproxy.Register(protocol)
		return protocol
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

func (c *testClient) getConn(ctx context.Context, primitive primitive.Type, name string) (*grpc.ClientConn, error) {
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

func (c *testClient) getOpts(opts ...primitive.Option) []primitive.Option {
	return append([]primitive.Option{primitive.WithSessionID(c.id)}, opts...)
}

func (c *testClient) GetCounter(ctx context.Context, name string, opts ...primitive.Option) (counter.Counter, error) {
	conn, err := c.getConn(ctx, counter.Type, name)
	if err != nil {
		return nil, err
	}
	return counter.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetElection(ctx context.Context, name string, opts ...primitive.Option) (election.Election, error) {
	conn, err := c.getConn(ctx, election.Type, name)
	if err != nil {
		return nil, err
	}
	return election.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetIndexedMap(ctx context.Context, name string, opts ...primitive.Option) (indexedmap.IndexedMap, error) {
	conn, err := c.getConn(ctx, indexedmap.Type, name)
	if err != nil {
		return nil, err
	}
	return indexedmap.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetList(ctx context.Context, name string, opts ...primitive.Option) (list.List, error) {
	conn, err := c.getConn(ctx, list.Type, name)
	if err != nil {
		return nil, err
	}
	return list.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetLock(ctx context.Context, name string, opts ...primitive.Option) (lock.Lock, error) {
	conn, err := c.getConn(ctx, lock.Type, name)
	if err != nil {
		return nil, err
	}
	return lock.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetMap(ctx context.Context, name string, opts ...primitive.Option) (_map.Map, error) {
	conn, err := c.getConn(ctx, _map.Type, name)
	if err != nil {
		return nil, err
	}
	return _map.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetSet(ctx context.Context, name string, opts ...primitive.Option) (set.Set, error) {
	conn, err := c.getConn(ctx, set.Type, name)
	if err != nil {
		return nil, err
	}
	return set.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) GetValue(ctx context.Context, name string, opts ...primitive.Option) (value.Value, error) {
	conn, err := c.getConn(ctx, value.Type, name)
	if err != nil {
		return nil, err
	}
	return value.New(ctx, name, conn, c.getOpts(opts...)...)
}

func (c *testClient) Close() error {
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
