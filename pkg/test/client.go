// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"fmt"
	counterv1 "github.com/atomix/atomix/api/runtime/counter/v1"
	electionv1 "github.com/atomix/atomix/api/runtime/election/v1"
	indexedmapv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	listv1 "github.com/atomix/atomix/api/runtime/list/v1"
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	runtimeapiv1 "github.com/atomix/atomix/api/runtime/v1"
	valuev1 "github.com/atomix/atomix/api/runtime/value/v1"
	rsmapiv1 "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/runtime/pkg/network"
	runtimev1 "github.com/atomix/atomix/runtime/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/utils/grpc/interceptors"
	"github.com/atomix/atomix/sidecar/pkg/sidecar"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"sync/atomic"
)

var PrimitiveTypes = []runtimeapiv1.PrimitiveType{
	counterv1.PrimitiveType,
	electionv1.PrimitiveType,
	indexedmapv1.PrimitiveType,
	listv1.PrimitiveType,
	lockv1.PrimitiveType,
	mapv1.PrimitiveType,
	setv1.PrimitiveType,
	valuev1.PrimitiveType,
}

func NewClient() *Client {
	return &Client{
		network: network.NewLocalDriver(),
		types:   PrimitiveTypes,
	}
}

type Client struct {
	network  network.Driver
	node     *node.Node
	types    []runtimeapiv1.PrimitiveType
	runtimes []*sidecar.Service
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

	runtime := runtimev1.New(runtimev1.WithDriver(driverID, newDriver(c.network)))

	storeID := runtimeapiv1.StoreID{
		Name: "test",
	}
	err := runtime.Program(ctx, runtimeapiv1.Route{
		StoreID: storeID,
		Rules: []runtimeapiv1.RoutingRule{
			{
				Names: []string{"*"},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	service := sidecar.NewService(runtime,
		sidecar.WithNetwork(c.network),
		sidecar.WithPort(nextPort())).(*sidecar.Service)
	if err := service.Start(); err != nil {
		return nil, err
	}
	c.runtimes = append(c.runtimes, service)

	config := rsmapiv1.ProtocolConfig{
		Partitions: []rsmapiv1.PartitionConfig{
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
	err = runtime.Connect(ctx, storeID, driverID, &types.Any{
		Value: []byte(data),
	})
	if err != nil {
		return nil, err
	}
	return c.connect(ctx, fmt.Sprintf(":%d", service.Port))
}

func (c *Client) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithContextDialer(c.network.Connect),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(
			interceptors.ErrorHandlingUnaryClientInterceptor(),
			interceptors.RetryingUnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(
			interceptors.ErrorHandlingStreamClientInterceptor(),
			interceptors.RetryingStreamClientInterceptor()))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *Client) Close() {
	for _, proxy := range c.runtimes {
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
