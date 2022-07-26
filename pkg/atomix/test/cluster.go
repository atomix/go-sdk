// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"fmt"
	multiraftv1 "github.com/atomix/multi-raft-storage/api/atomix/multiraft/v1"
	"github.com/atomix/multi-raft-storage/driver"
	proxyv1 "github.com/atomix/runtime/api/atomix/proxy/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	counterv1 "github.com/atomix/runtime/sdk/primitives/counter/v1"
	mapv1 "github.com/atomix/runtime/sdk/primitives/map/v1"
	"github.com/gogo/protobuf/jsonpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
)

func NewCluster(replicas int, partitions int) *Cluster {
	config := getClusterConfig(replicas, partitions)
	network := proxy.NewLocalNetwork()
	return &Cluster{
		types:   []runtime.Type{counterv1.Type, mapv1.Type},
		network: network,
		port:    5000,
		config:  config,
	}
}

type Cluster struct {
	types   []runtime.Type
	network proxy.Network
	proxies []*proxy.Proxy
	config  multiraftv1.ClusterConfig
	nodes   map[multiraftv1.NodeID]*Node
	port    int
}

func (c *Cluster) Nodes() []*Node {
	nodes := make([]*Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (c *Cluster) start() error {
	if err := os.RemoveAll("test-data"); err != nil && !os.IsNotExist(err) {
		return err
	}
	if c.nodes != nil {
		return nil
	}
	c.nodes = make(map[multiraftv1.NodeID]*Node)
	for _, replica := range c.config.Replicas {
		node := newNode(c, replica.NodeID)
		if err := node.Start(); err != nil {
			return err
		}
		c.nodes[node.id] = node
	}
	return nil
}

func (c *Cluster) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	if err := c.start(); err != nil {
		return nil, err
	}

	proxyPort := c.port
	c.port++
	runtimePort := c.port
	c.port++

	driver := driver.New(c.network)
	proxy := proxy.New(c.network,
		proxy.WithDrivers(driver),
		proxy.WithTypes(c.types...),
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
	c.proxies = append(c.proxies, proxy)

	conn, err := c.connect(ctx, fmt.Sprintf(":%d", proxy.ProxyService.Port))
	if err != nil {
		return nil, err
	}

	client := proxyv1.NewProxyClient(conn)

	marshaller := &jsonpb.Marshaler{}
	data, err := marshaller.MarshalToString(&c.config)
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

func (c *Cluster) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithContextDialer(c.network.Connect),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return conn, nil
}

func (c *Cluster) Cleanup() {
	for _, proxy := range c.proxies {
		_ = proxy.Stop()
	}
	for _, node := range c.nodes {
		_ = node.Stop()
	}
	_ = os.RemoveAll("test-data")
}

func getClusterConfig(numReplicas, numPartitions int) multiraftv1.ClusterConfig {
	var config multiraftv1.ClusterConfig
	for i := 1; i <= numReplicas; i++ {
		config.Replicas = append(config.Replicas, multiraftv1.ReplicaConfig{
			NodeID:   multiraftv1.NodeID(i),
			Host:     "localhost",
			ApiPort:  5680 + int32(i),
			RaftPort: 5690 + int32(i),
		})
	}
	for i := 1; i <= numPartitions; i++ {
		var members []multiraftv1.MemberConfig
		for j := 1; j <= numReplicas; j++ {
			members = append(members, multiraftv1.MemberConfig{
				NodeID: multiraftv1.NodeID(j),
			})
		}
		config.Partitions = append(config.Partitions, multiraftv1.PartitionConfig{
			PartitionID: multiraftv1.PartitionID(i),
			Host:        "localhost",
			Port:        5681,
			Members:     members,
		})
	}
	return config
}
