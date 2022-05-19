// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"fmt"
	primitiveapi "github.com/atomix/api/pkg/atomix"
	driverapi "github.com/atomix/api/pkg/atomix/management/driver"
	protocolapi "github.com/atomix/api/pkg/atomix/protocol"
	rsmcounterprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/counter"
	rsmelectionprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/election"
	rsmindexedmapprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/indexedmap"
	rsmlistprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/list"
	rsmlockprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/lock"
	rsmmapprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/map"
	rsmsetprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/set"
	rsmvalueprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/value"
	"github.com/atomix/atomix-go-local/pkg/atomix/local"
	"github.com/atomix/runtime/pkg/cluster"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/driver/env"
	"github.com/atomix/runtime/pkg/driver/proxy"
	rsmdriver "github.com/atomix/runtime/pkg/driver/proxy/rsm"
	rsmcounterproxy "github.com/atomix/runtime/pkg/driver/proxy/rsm/counter"
	rsmelectionproxy "github.com/atomix/runtime/pkg/driver/proxy/rsm/election"
	rsmindexedmapproxy "github.com/atomix/runtime/pkg/driver/proxy/rsm/indexedmap"
	rsmlistproxy "github.com/atomix/runtime/pkg/driver/proxy/rsm/list"
	rsmlockproxy "github.com/atomix/runtime/pkg/driver/proxy/rsm/lock"
	rsmmapproxy "github.com/atomix/runtime/pkg/driver/proxy/rsm/map"
	rsmsetproxy "github.com/atomix/runtime/pkg/driver/proxy/rsm/set"
	rsmvalueproxy "github.com/atomix/runtime/pkg/driver/proxy/rsm/value"
	rsmprotocol "github.com/atomix/runtime/pkg/storage/protocol/rsm"
	"google.golang.org/grpc"
)

// NewRSMTest creates a new RSM-based test
func NewRSMTest() *RSMTest {
	return &RSMTest{
		network: cluster.NewLocalNetwork(),
		config: protocolapi.ProtocolConfig{
			Replicas: []protocolapi.ProtocolReplica{
				{
					ID:      "rsm-1",
					NodeID:  "node-1",
					APIPort: 7001,
				},
			},
			Partitions: []protocolapi.ProtocolPartition{
				{
					PartitionID: 1,
					Replicas:    []string{"rsm-1"},
					APIPort:     7001,
				},
			},
		},
	}
}

// RSMTest is an RSM-based primitive test
type RSMTest struct {
	network  cluster.Network
	config   protocolapi.ProtocolConfig
	protocol *rsmprotocol.Node
	drivers  []*driver.Driver
}

// Start starts the test cluster
func (t *RSMTest) Start() error {
	t.protocol = rsmprotocol.NewNode(
		cluster.NewCluster(
			t.network,
			t.config,
			cluster.WithMemberID("rsm-1")),
		local.NewProtocol())
	rsmcounterprotocol.RegisterService(t.protocol)
	rsmelectionprotocol.RegisterService(t.protocol)
	rsmindexedmapprotocol.RegisterService(t.protocol)
	rsmlistprotocol.RegisterService(t.protocol)
	rsmlockprotocol.RegisterService(t.protocol)
	rsmmapprotocol.RegisterService(t.protocol)
	rsmsetprotocol.RegisterService(t.protocol)
	rsmvalueprotocol.RegisterService(t.protocol)
	err := t.protocol.Start()
	if err != nil {
		return err
	}
	return nil
}

// CreateProxy creates an RSM proxy and returns the connection
func (t *RSMTest) CreateProxy(primitiveID primitiveapi.PrimitiveId) (*grpc.ClientConn, error) {
	protocolFunc := func(rsmCluster cluster.Cluster, driverEnv env.DriverEnv) proxy.Protocol {
		protocol := rsmdriver.NewProtocol(rsmCluster, driverEnv)
		rsmcounterproxy.Register(protocol)
		rsmelectionproxy.Register(protocol)
		rsmindexedmapproxy.Register(protocol)
		rsmlistproxy.Register(protocol)
		rsmlockproxy.Register(protocol)
		rsmmapproxy.Register(protocol)
		rsmsetproxy.Register(protocol)
		rsmvalueproxy.Register(protocol)
		return protocol
	}

	driverPort := 5252 + len(t.drivers)
	driver := driver.NewDriver(
		cluster.NewCluster(
			t.network,
			protocolapi.ProtocolConfig{},
			cluster.WithMemberID("rsm"),
			cluster.WithPort(driverPort)),
		protocolFunc,
		driver.WithNamespace("test"))
	err := driver.Start()
	if err != nil {
		return nil, err
	}
	t.drivers = append(t.drivers, driver)

	driverConn, err := grpc.Dial(fmt.Sprintf(":%d", driverPort), grpc.WithInsecure(), grpc.WithContextDialer(t.network.Connect))
	if err != nil {
		return nil, err
	}
	defer driverConn.Close()
	driverClient := driverapi.NewDriverClient(driverConn)

	agentPort := int32(55680 + len(t.drivers))
	agentID := driverapi.AgentId{
		Namespace: "test",
		Name:      "rsm",
	}
	agentAddress := driverapi.AgentAddress{
		Host: "localhost",
		Port: agentPort,
	}
	agentConfig := driverapi.AgentConfig{
		Protocol: t.config,
	}

	_, err = driverClient.StartAgent(context.TODO(), &driverapi.StartAgentRequest{AgentID: agentID, Address: agentAddress, Config: agentConfig})
	if err != nil {
		return nil, err
	}

	agentConn, err := grpc.Dial(fmt.Sprintf(":%d", agentPort), grpc.WithInsecure(), grpc.WithContextDialer(t.network.Connect))
	if err != nil {
		return nil, err
	}
	agentClient := driverapi.NewAgentClient(agentConn)

	proxyOptions := driverapi.ProxyOptions{
		Read:  true,
		Write: true,
	}
	_, err = agentClient.CreateProxy(context.TODO(), &driverapi.CreateProxyRequest{ProxyID: driverapi.ProxyId{PrimitiveId: primitiveID}, Options: proxyOptions})
	if err != nil {
		return nil, err
	}
	return agentConn, nil
}

// Stop stops the RSM test cluster
func (t *RSMTest) Stop() error {
	for _, driver := range t.drivers {
		err := driver.Stop()
		if err != nil {
			return err
		}
	}

	if t.protocol != nil {
		err := t.protocol.Stop()
		if err != nil {
			return err
		}
	}
	return nil
}
