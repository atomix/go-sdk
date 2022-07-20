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
	counterv1 "github.com/atomix/multi-raft-storage/node/pkg/primitive/counter/v1"
	proxyv1 "github.com/atomix/runtime/api/atomix/proxy/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func New(t runtime.Type) *Test {
	network := proxy.NewLocalNetwork()
	return &Test{
		types:   []runtime.Type{t},
		network: network,
		port:    5000,
	}
}

type Test struct {
	types   []runtime.Type
	network proxy.Network
	proxies []*proxy.Proxy
	node    *node.MultiRaftNode
	port    int
}

func (t *Test) start() error {
	if t.node != nil {
		return nil
	}
	t.node = node.New(t.network,
		node.WithNodeID(1),
		node.WithConfig(multiraftv1.MultiRaftConfig{
			DataDir: "test-data",
		}),
		node.WithPrimitiveTypes(counterv1.Type))
	return t.node.Start()
}

func (t *Test) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	if err := t.start(); err != nil {
		return nil, err
	}

	proxyPort := t.port
	t.port++
	runtimePort := t.port
	t.port++

	driver := driver.New(t.network)
	proxy := proxy.New(t.network,
		proxy.WithDrivers(driver),
		proxy.WithTypes(t.types...),
		proxy.WithProxyPort(proxyPort),
		proxy.WithRuntimePort(runtimePort),
		proxy.WithRouterConfig(proxy.RouterConfig{
			Routes: []proxy.RouteConfig{
				{
					Store: proxy.StoreID{
						Name: "test",
					},
					Rules: []proxy.RuleConfig{
						{
							Kinds:       []string{"*"},
							APIVersions: []string{"*"},
							Names:       []string{"*"},
						},
					},
				},
			},
		}))

	if err := proxy.Start(); err != nil {
		return nil, err
	}
	t.proxies = append(t.proxies, proxy)

	conn, err := t.connect(ctx, fmt.Sprintf(":%d", proxy.ProxyService.Port))
	if err != nil {
		return nil, err
	}

	client := proxyv1.NewProxyClient(conn)

	config := multiraftv1.ClusterConfig{
		Replicas: []multiraftv1.ReplicaConfig{
			{
				NodeID: 1,
				Port:   5680,
			},
		},
		Partitions: []multiraftv1.PartitionConfig{
			{
				PartitionID: 1,
				Port:        5681,
				Members: []multiraftv1.MemberConfig{
					{
						NodeID: 1,
					},
				},
			},
			{
				PartitionID: 2,
				Port:        5682,
				Members: []multiraftv1.MemberConfig{
					{
						NodeID: 1,
					},
				},
			},
			{
				PartitionID: 3,
				Port:        5683,
				Members: []multiraftv1.MemberConfig{
					{
						NodeID: 1,
					},
				},
			},
		},
	}
	bytes, err := proto.Marshal(&config)
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
		Config: bytes,
	}
	_, err = client.Connect(ctx, request)
	if err != nil {
		return nil, err
	}
	return t.connect(ctx, fmt.Sprintf(":%d", proxy.RuntimeService.Port))
}

func (t *Test) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithContextDialer(t.network.Connect),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return conn, nil
}

func (t *Test) Cleanup() {
	for _, proxy := range t.proxies {
		_ = proxy.Stop()
	}
	if t.node != nil {
		_ = t.node.Stop()
	}
}
