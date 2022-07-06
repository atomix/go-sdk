// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"fmt"
	"github.com/atomix/drivers/memory"
	proxyv1 "github.com/atomix/proxy/api/atomix/proxy/v1"
	"github.com/atomix/proxy/pkg/proxy"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func New(t runtime.Type) *Test {
	return &Test{
		types:   []runtime.Type{t},
		network: proxy.NewLocalNetwork(),
		driver:  memory.New(),
		port:    5000,
	}
}

type Test struct {
	types   []runtime.Type
	network proxy.Network
	driver  runtime.Driver
	proxies []*proxy.Proxy
	port    int
}

func (t *Test) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	proxyPort := t.port
	t.port++
	runtimePort := t.port
	t.port++

	proxy := proxy.New(t.network,
		proxy.WithDrivers(t.driver),
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

	request := &proxyv1.ConnectRequest{
		StoreID: proxyv1.StoreId{
			Name: "test",
		},
		DriverID: proxyv1.DriverId{
			Name:    memory.Driver.ID().Name,
			Version: memory.Driver.ID().Version,
		},
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
}
