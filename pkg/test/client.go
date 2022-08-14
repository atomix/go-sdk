// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"fmt"
	multiraftv1 "github.com/atomix/multi-raft-storage/api/atomix/multiraft/v1"
	"github.com/atomix/multi-raft-storage/driver"
	"github.com/atomix/multi-raft-storage/node/pkg/node"
	proxyv1 "github.com/atomix/runtime/api/atomix/proxy/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	atomiccounterv1 "github.com/atomix/runtime/sdk/pkg/runtime/atomic/counter/v1"
	atomicmapv1 "github.com/atomix/runtime/sdk/pkg/runtime/atomic/map/v1"
	counterv1 "github.com/atomix/runtime/sdk/pkg/runtime/counter/v1"
	mapv1 "github.com/atomix/runtime/sdk/pkg/runtime/map/v1"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"sync/atomic"
)

var Types = []runtime.Type{
	atomiccounterv1.Type,
	atomicmapv1.Type,
	counterv1.Type,
	mapv1.Type,
}

func NewClient() *Client {
	network := proxy.NewLocalNetwork()
	return &Client{
		network: network,
		types:   Types,
	}
}

type Client struct {
	network proxy.Network
	node    *node.MultiRaftNode
	types   []runtime.Type
	proxies []*proxy.Proxy
}

func (c *Client) start() error {
	if c.node != nil {
		return nil
	}

	raftPort := int32(nextPort())
	c.node = node.New(c.network,
		node.WithHost("localhost"),
		node.WithPort(nextPort()),
		node.WithConfig(multiraftv1.NodeConfig{
			Host: "localhost",
			Port: raftPort,
			MultiRaftConfig: multiraftv1.MultiRaftConfig{
				DataDir: "test-data",
			},
		}))
	if err := c.node.Start(); err != nil {
		return err
	}
	println(fmt.Sprintf("PORT: %d", c.node.Port))

	conn, err := c.connect(context.Background(), fmt.Sprintf("%s:%d", c.node.Host, c.node.Port))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := multiraftv1.NewNodeClient(conn)
	_, err = client.Bootstrap(context.Background(), &multiraftv1.BootstrapRequest{
		Group: multiraftv1.GroupConfig{
			GroupID:  1,
			MemberID: 1,
			Role:     multiraftv1.MemberRole_MEMBER,
			Members: []multiraftv1.MemberConfig{
				{
					MemberID: 1,
					Host:     "localhost",
					Port:     raftPort,
				},
			},
		},
	})
	if err != nil {
		return err
	}
	_, err = client.Bootstrap(context.Background(), &multiraftv1.BootstrapRequest{
		Group: multiraftv1.GroupConfig{
			GroupID:  2,
			MemberID: 1,
			Role:     multiraftv1.MemberRole_MEMBER,
			Members: []multiraftv1.MemberConfig{
				{
					MemberID: 1,
					Host:     "localhost",
					Port:     raftPort,
				},
			},
		},
	})
	if err != nil {
		return err
	}
	_, err = client.Bootstrap(context.Background(), &multiraftv1.BootstrapRequest{
		Group: multiraftv1.GroupConfig{
			GroupID:  3,
			MemberID: 1,
			Role:     multiraftv1.MemberRole_MEMBER,
			Members: []multiraftv1.MemberConfig{
				{
					MemberID: 1,
					Host:     "localhost",
					Port:     raftPort,
				},
			},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	if err := c.start(); err != nil {
		return nil, err
	}

	driver := driver.New(c.network)
	proxy := proxy.New(c.network,
		proxy.WithDrivers(driver),
		proxy.WithTypes(c.types...),
		proxy.WithProxyPort(nextPort()),
		proxy.WithRuntimePort(nextPort()),
		proxy.WithRouterConfig(proxy.RouterConfig{
			Routes: []proxy.RouteConfig{
				{
					Store: proxy.StoreID{
						Name: "test",
					},
					Bindings: []proxy.BindingConfig{
						{
							Services: []proxy.ServiceConfig{},
							MatchRules: []proxy.MatchRuleConfig{
								{
									Names: []string{"*"},
								},
							},
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

	config := multiraftv1.DriverConfig{
		Partitions: []multiraftv1.PartitionConfig{
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

func (c *Client) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithContextDialer(c.network.Connect),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return conn, nil
}

func (c *Client) Cleanup() {
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
