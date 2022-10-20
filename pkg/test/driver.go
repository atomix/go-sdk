// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	counterv1api "github.com/atomix/runtime/api/atomix/runtime/counter/v1"
	countermapv1api "github.com/atomix/runtime/api/atomix/runtime/countermap/v1"
	electionv1api "github.com/atomix/runtime/api/atomix/runtime/election/v1"
	indexedmapv1api "github.com/atomix/runtime/api/atomix/runtime/indexedmap/v1"
	lockv1api "github.com/atomix/runtime/api/atomix/runtime/lock/v1"
	mapv1api "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	multimapv1api "github.com/atomix/runtime/api/atomix/runtime/multimap/v1"
	setv1api "github.com/atomix/runtime/api/atomix/runtime/set/v1"
	valuev1api "github.com/atomix/runtime/api/atomix/runtime/value/v1"
	counterv1 "github.com/atomix/runtime/primitives/pkg/counter/v1"
	countermapv1 "github.com/atomix/runtime/primitives/pkg/countermap/v1"
	electionv1 "github.com/atomix/runtime/primitives/pkg/election/v1"
	indexedmapv1 "github.com/atomix/runtime/primitives/pkg/indexedmap/v1"
	lockv1 "github.com/atomix/runtime/primitives/pkg/lock/v1"
	mapv1 "github.com/atomix/runtime/primitives/pkg/map/v1"
	multimapv1 "github.com/atomix/runtime/primitives/pkg/multimap/v1"
	setv1 "github.com/atomix/runtime/primitives/pkg/set/v1"
	valuev1 "github.com/atomix/runtime/primitives/pkg/value/v1"
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

func newConn(network network.Network) *testConn {
	return &testConn{
		ProtocolClient: client.NewClient(network),
	}
}

type testConn struct {
	*client.ProtocolClient
}

func (c *testConn) Connect(ctx context.Context, spec runtime.ConnSpec) error {
	var config protocol.ProtocolConfig
	if err := spec.UnmarshalConfig(&config); err != nil {
		return err
	}
	return c.ProtocolClient.Connect(ctx, config)
}

func (c *testConn) Configure(ctx context.Context, spec runtime.ConnSpec) error {
	var config protocol.ProtocolConfig
	if err := spec.UnmarshalConfig(&config); err != nil {
		return err
	}
	return c.ProtocolClient.Configure(ctx, config)
}

func (c *testConn) NewCounter(spec runtime.PrimitiveSpec) (counterv1api.CounterServer, error) {
	return counterv1.NewCounterProxy(c.Protocol, spec)
}

func (c *testConn) NewCounterMap(spec runtime.PrimitiveSpec) (countermapv1api.CounterMapServer, error) {
	return countermapv1.NewCounterMapProxy(c.Protocol, spec)
}

func (c *testConn) NewLeaderElection(spec runtime.PrimitiveSpec) (electionv1api.LeaderElectionServer, error) {
	return electionv1.NewLeaderElectionProxy(c.Protocol, spec)
}

func (c *testConn) NewIndexedMap(spec runtime.PrimitiveSpec) (indexedmapv1api.IndexedMapServer, error) {
	return indexedmapv1.NewIndexedMapProxy(c.Protocol, spec)
}

func (c *testConn) NewLock(spec runtime.PrimitiveSpec) (lockv1api.LockServer, error) {
	return lockv1.NewLockProxy(c.Protocol, spec)
}

func (c *testConn) NewMap(spec runtime.PrimitiveSpec) (mapv1api.MapServer, error) {
	return mapv1.NewMapProxy(c.Protocol, spec)
}

func (c *testConn) NewMultiMap(spec runtime.PrimitiveSpec) (multimapv1api.MultiMapServer, error) {
	return multimapv1.NewMultiMapProxy(c.Protocol, spec)
}

func (c *testConn) NewSet(spec runtime.PrimitiveSpec) (setv1api.SetServer, error) {
	return setv1.NewSetProxy(c.Protocol, spec)
}

func (c *testConn) NewValue(spec runtime.PrimitiveSpec) (valuev1api.ValueServer, error) {
	return valuev1.NewValueProxy(c.Protocol, spec)
}
