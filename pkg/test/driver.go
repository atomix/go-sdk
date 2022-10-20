// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	counterv1api "github.com/atomix/runtime/api/atomix/runtime/counter/v1"
	counterv1 "github.com/atomix/runtime/primitives/pkg/counter/v1"
	"github.com/atomix/runtime/sdk/pkg/network"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/client"
	"github.com/atomix/runtime/sdk/pkg/runtime"
)

var driverID = runtime.DriverID{
	Name:    "Test",
	Version: "v1",
}

func newDriver(network network.Network) runtime.Driver {
	return &testDriver{
		network: network,
	}
}

type testDriver struct {
	network network.Network
}

func (d *testDriver) ID() runtime.DriverID {
	return driverID
}

func (d *testDriver) Connect(ctx context.Context, spec runtime.ConnSpec) (runtime.Conn, error) {
	conn := newConn(d.network)
	if err := conn.Connect(ctx, spec); err != nil {
		return nil, err
	}
	return conn, nil
}

func (d *testDriver) String() string {
	return d.ID().String()
}

func newConn(network network.Network) *multiRaftConn {
	return &multiRaftConn{
		ProtocolClient: client.NewClient(network),
	}
}

type multiRaftConn struct {
	*client.ProtocolClient
}

func (c *multiRaftConn) Connect(ctx context.Context, spec runtime.ConnSpec) error {
	var config protocol.ProtocolConfig
	if err := spec.UnmarshalConfig(&config); err != nil {
		return err
	}
	return c.ProtocolClient.Connect(ctx, config)
}

func (c *multiRaftConn) Configure(ctx context.Context, spec runtime.ConnSpec) error {
	var config protocol.ProtocolConfig
	if err := spec.UnmarshalConfig(&config); err != nil {
		return err
	}
	return c.ProtocolClient.Configure(ctx, config)
}

func (c *multiRaftConn) NewCounter(spec runtime.PrimitiveSpec) (counterv1api.CounterServer, error) {
	return counterv1.NewCounterProxy(c.Protocol, spec)
}
