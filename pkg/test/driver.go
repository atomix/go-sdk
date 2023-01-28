// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	runtimeapiv1 "github.com/atomix/atomix/api/runtime/v1"
	rsmapiv1 "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	counterclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/counter/v1"
	countermapclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/countermap/v1"
	electionclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/election/v1"
	indexedmapclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/indexedmap/v1"
	lockclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/lock/v1"
	mapclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/map/v1"
	multimapclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/multimap/v1"
	setclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/set/v1"
	valueclientv1 "github.com/atomix/atomix/protocols/rsm/pkg/client/value/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/network"
	runtimecounterv1 "github.com/atomix/atomix/runtime/pkg/runtime/counter/v1"
	runtimecountermapv1 "github.com/atomix/atomix/runtime/pkg/runtime/countermap/v1"
	runtimeelectionv1 "github.com/atomix/atomix/runtime/pkg/runtime/election/v1"
	runtimeindexedmapv1 "github.com/atomix/atomix/runtime/pkg/runtime/indexedmap/v1"
	runtimelockv1 "github.com/atomix/atomix/runtime/pkg/runtime/lock/v1"
	runtimemapv1 "github.com/atomix/atomix/runtime/pkg/runtime/map/v1"
	runtimemultimapv1 "github.com/atomix/atomix/runtime/pkg/runtime/multimap/v1"
	runtimesetv1 "github.com/atomix/atomix/runtime/pkg/runtime/set/v1"
	runtimevaluev1 "github.com/atomix/atomix/runtime/pkg/runtime/value/v1"
)

var driverID = runtimeapiv1.DriverID{
	Name:       "Test",
	APIVersion: "v1",
}

func newDriver(network network.Driver) driver.Driver {
	return &testDriver{
		network: network,
	}
}

type testDriver struct {
	network network.Driver
}

func (d *testDriver) Connect(ctx context.Context, spec rsmapiv1.ProtocolConfig) (driver.Conn, error) {
	conn := newConn(d.network)
	if err := conn.Connect(ctx, spec); err != nil {
		return nil, err
	}
	return conn, nil
}

func newConn(network network.Driver) *testConn {
	return &testConn{
		ProtocolClient: client.NewClient(network),
	}
}

type testConn struct {
	*client.ProtocolClient
}

func (c *testConn) Connect(ctx context.Context, spec rsmapiv1.ProtocolConfig) error {
	return c.ProtocolClient.Connect(ctx, spec)
}

func (c *testConn) Configure(ctx context.Context, spec rsmapiv1.ProtocolConfig) error {
	return c.ProtocolClient.Configure(ctx, spec)
}

func (c *testConn) NewCounterV1(ctx context.Context, id runtimeapiv1.PrimitiveID) (runtimecounterv1.CounterProxy, error) {
	proxy := counterclientv1.NewCounter(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *testConn) NewCounterMapV1(ctx context.Context, id runtimeapiv1.PrimitiveID) (runtimecountermapv1.CounterMapProxy, error) {
	proxy := countermapclientv1.NewCounterMap(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *testConn) NewLeaderElectionV1(ctx context.Context, id runtimeapiv1.PrimitiveID) (runtimeelectionv1.LeaderElectionProxy, error) {
	proxy := electionclientv1.NewLeaderElection(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *testConn) NewIndexedMapV1(ctx context.Context, id runtimeapiv1.PrimitiveID) (runtimeindexedmapv1.IndexedMapProxy, error) {
	proxy := indexedmapclientv1.NewIndexedMap(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *testConn) NewLockV1(ctx context.Context, id runtimeapiv1.PrimitiveID) (runtimelockv1.LockProxy, error) {
	proxy := lockclientv1.NewLock(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *testConn) NewMapV1(ctx context.Context, id runtimeapiv1.PrimitiveID) (runtimemapv1.MapProxy, error) {
	proxy := mapclientv1.NewMap(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *testConn) NewMultiMapV1(ctx context.Context, id runtimeapiv1.PrimitiveID) (runtimemultimapv1.MultiMapProxy, error) {
	proxy := multimapclientv1.NewMultiMap(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *testConn) NewSetV1(ctx context.Context, id runtimeapiv1.PrimitiveID) (runtimesetv1.SetProxy, error) {
	proxy := setclientv1.NewSet(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

func (c *testConn) NewValueV1(ctx context.Context, id runtimeapiv1.PrimitiveID) (runtimevaluev1.ValueProxy, error) {
	proxy := valueclientv1.NewValue(c.Protocol, id)
	if err := proxy.Open(ctx); err != nil {
		return nil, err
	}
	return proxy, nil
}

var _ runtimecounterv1.CounterProvider = (*testConn)(nil)
var _ runtimecountermapv1.CounterMapProvider = (*testConn)(nil)
var _ runtimeelectionv1.LeaderElectionProvider = (*testConn)(nil)
var _ runtimeindexedmapv1.IndexedMapProvider = (*testConn)(nil)
var _ runtimelockv1.LockProvider = (*testConn)(nil)
var _ runtimemapv1.MapProvider = (*testConn)(nil)
var _ runtimemultimapv1.MultiMapProvider = (*testConn)(nil)
var _ runtimesetv1.SetProvider = (*testConn)(nil)
var _ runtimevaluev1.ValueProvider = (*testConn)(nil)
