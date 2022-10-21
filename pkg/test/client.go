// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"fmt"
	proxyv1 "github.com/atomix/runtime/api/atomix/proxy/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	counterv1 "github.com/atomix/runtime/proxy/pkg/proxy/counter/v1"
	mapv1 "github.com/atomix/runtime/proxy/pkg/proxy/map/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/network"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/node"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"sync/atomic"
)

var Types = []proxy.Type{
	counterv1.Type,
	mapv1.Type,
}

func NewClient() *Client {
	network := network.NewLocalNetwork()
	return &Client{
		network: network,
		types:   Types,
	}
}

type Client struct {
	network network.Network
	node    *node.Node
	types   []proxy.Type
	proxies []*proxy.Proxy
}

func (c *Client) start() error {
	if c.node != nil {
		return nil
	}
	c.node = newNode(c.network, node.WithHost("localhost"), node.WithPort(nextPort()))
	if err := c.node.Start(); err != nil {
		return err
	}
	return nil
}

func (c *Client) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	if err := c.start(); err != nil {
		return nil, err
	}

	proxy := proxy.New(c.network,
		proxy.WithDrivers(newDriver(c.network)),
		proxy.WithTypes(c.types...),
		proxy.WithProxyPort(nextPort()),
		proxy.WithRuntimePort(nextPort()),
		proxy.WithConfig(
			proxy.Config{
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
			Name:    driverID.Name,
			Version: driverID.Version,
		},
		Config: []byte(data),
	}
	_, err = client.Connect(ctx, request)
	if err != nil {
		return nil, err
	}
	return c.connect(ctx, fmt.Sprintf(":%d", proxy.RuntimeService.Port))
}

func (c *Client) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithContextDialer(c.network.Connect),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return conn, nil
}

func (c *Client) Close() {
	for _, proxy := range c.proxies {
		_ = proxy.Stop()
	}
	_ = c.node.Stop()
	_ = os.RemoveAll("test-data")
}

var port = &atomic.Int32{}

func init() {
	port.Store(5000)
}

func nextPort() int {
	return int(port.Add(1))
}
