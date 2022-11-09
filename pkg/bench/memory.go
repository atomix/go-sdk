// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package bench

import (
	"context"
	"fmt"
	"github.com/atomix/go-sdk/pkg/primitive"
	proxyv1 "github.com/atomix/runtime/api/atomix/proxy/v1"
	counterv1 "github.com/atomix/runtime/primitives/pkg/counter/v1"
	electionv1 "github.com/atomix/runtime/primitives/pkg/election/v1"
	indexedmapv1 "github.com/atomix/runtime/primitives/pkg/indexedmap/v1"
	lockv1 "github.com/atomix/runtime/primitives/pkg/lock/v1"
	mapv1 "github.com/atomix/runtime/primitives/pkg/map/v1"
	setv1 "github.com/atomix/runtime/primitives/pkg/set/v1"
	valuev1 "github.com/atomix/runtime/primitives/pkg/value/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/network"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/node"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/atomix/shared-memory-storage/driver"
	"github.com/atomix/shared-memory-storage/node/pkg/sharedmemory"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
)

func NewMemoryBenchmark() primitive.Client {
	_ = os.RemoveAll("test-data")
	network := network.NewLocalNetwork()
	return &memoryBenchmark{
		network: network,
		types:   Types,
	}
}

type memoryBenchmark struct {
	network network.Network
	node    *node.Node
	types   []proxy.Type
	proxies []*proxy.Proxy
}

func (c *memoryBenchmark) start() error {
	if c.node != nil {
		return nil
	}

	registry := statemachine.NewPrimitiveTypeRegistry()
	counterv1.RegisterStateMachine(registry)
	electionv1.RegisterStateMachine(registry)
	indexedmapv1.RegisterStateMachine(registry)
	lockv1.RegisterStateMachine(registry)
	mapv1.RegisterStateMachine(registry)
	setv1.RegisterStateMachine(registry)
	valuev1.RegisterStateMachine(registry)

	c.node = node.NewNode(
		c.network,
		sharedmemory.NewProtocol(),
		node.WithHost("localhost"),
		node.WithPort(nextPort()))

	counterv1.RegisterServer(c.node)
	electionv1.RegisterServer(c.node)
	indexedmapv1.RegisterServer(c.node)
	lockv1.RegisterServer(c.node)
	mapv1.RegisterServer(c.node)
	setv1.RegisterServer(c.node)
	valuev1.RegisterServer(c.node)

	if err := c.node.Start(); err != nil {
		return err
	}
	println(fmt.Sprintf("PORT: %d", c.node.Port))
	return nil
}

func (c *memoryBenchmark) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	if err := c.start(); err != nil {
		return nil, err
	}

	driver := driver.New(c.network)
	proxy := proxy.New(c.network,
		proxy.WithDrivers(driver),
		proxy.WithTypes(c.types...),
		proxy.WithProxyPort(nextPort()),
		proxy.WithRuntimePort(nextPort()),
		proxy.WithConfig(proxy.Config{
			Router: proxy.RouterConfig{
				Routes: []proxy.RouteConfig{
					{
						Store: proxy.StoreID{
							Name: "test",
						},
					},
				},
			},
		}))

	if err := proxy.Start(); err != nil {
		return nil, err
	}
	c.proxies = append(c.proxies, proxy)

	conn, err := c.connect(ctx, fmt.Sprintf(":%d", proxy.ProxyService.Port))
	if err != nil {
		return nil, err
	}

	client := proxyv1.NewProxyClient(conn)

	config := protocol.ProtocolConfig{
		Partitions: []protocol.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      fmt.Sprintf("localhost:%d", c.node.Port),
			},
			{
				PartitionID: 2,
				Leader:      fmt.Sprintf("localhost:%d", c.node.Port),
			},
			{
				PartitionID: 3,
				Leader:      fmt.Sprintf("localhost:%d", c.node.Port),
			},
		},
	}

	marshaller := &jsonpb.Marshaler{}
	data, err := marshaller.MarshalToString(&config)
	if err != nil {
		return nil, err
	}

	request := &proxyv1.ConnectRequest{
		StoreID: proxyv1.StoreId{
			Name: "test",
		},
		DriverID: proxyv1.DriverId{
			Name:    driver.ID().Name,
			Version: driver.ID().Version,
		},
		Config: []byte(data),
	}
	_, err = client.Connect(ctx, request)
	if err != nil {
		return nil, err
	}
	return c.connect(ctx, fmt.Sprintf(":%d", proxy.RuntimeService.Port))
}

func (c *memoryBenchmark) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithContextDialer(c.network.Connect),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return conn, nil
}

func (c *memoryBenchmark) Close() {
	for _, proxy := range c.proxies {
		_ = proxy.Stop()
	}
	_ = c.node.Stop()
	_ = os.RemoveAll("test-data")
}
