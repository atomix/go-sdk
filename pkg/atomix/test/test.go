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
	"google.golang.org/grpc/metadata"
	"time"
)

const (
	storeName = "test-cluster"
	appName   = "test-app"
	envKey    = "Environment"
	testEnv   = "test"
)

func Context() context.Context {
	return metadata.AppendToOutgoingContext(context.TODO(), envKey, testEnv)
}

func NewRuntime(t runtime.Type) *Runtime {
	network := proxy.NewLocalNetwork()
	return &Runtime{
		network: network,
		proxy: proxy.New(
			network,
			proxy.WithDrivers(memory.NewDriver()),
			proxy.WithTypes(t)),
	}
}

type Runtime struct {
	network proxy.Network
	proxy   *proxy.Proxy
}

func (r *Runtime) Start() error {
	if err := r.proxy.Start(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := r.connectProxy(ctx); err != nil {
		return err
	}
	return nil
}

func (r *Runtime) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	target := fmt.Sprintf(":%d", r.proxy.RuntimeService.Port)
	return r.connect(ctx, target)
}

func (r *Runtime) connectProxy(ctx context.Context) error {
	target := fmt.Sprintf(":%d", r.proxy.ProxyService.Port)
	conn, err := r.connect(ctx, target)
	if err != nil {
		return err
	}

	client := proxyv1.NewProxyClient(conn)

	request := &proxyv1.ConnectRequest{
		StoreID: proxyv1.StoreId{
			Name: storeName,
		},
		DriverID: proxyv1.DriverId{
			Name:    memory.Driver.Name(),
			Version: memory.Driver.Version(),
		},
	}
	_, err = client.Connect(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}
	return nil
}

func (r *Runtime) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithContextDialer(r.network.Connect),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return conn, nil
}
