// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	rsmprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	rsmcounterprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/counter"
	rsmelectionprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/election"
	rsmindexedmapprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/indexedmap"
	rsmlistprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/list"
	rsmlockprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/lock"
	rsmmapprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/map"
	rsmsetprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/set"
	rsmvalueprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm/value"
	"github.com/atomix/atomix-go-local/pkg/atomix/local"
)

func newReplica(network cluster.Network, replica protocolapi.ProtocolReplica, protocol protocolapi.ProtocolConfig) *testReplica {
	return &testReplica{
		network:  network,
		replica:  replica,
		protocol: protocol,
	}
}

type testReplica struct {
	network  cluster.Network
	replica  protocolapi.ProtocolReplica
	protocol protocolapi.ProtocolConfig
	node     *rsmprotocol.Node
}

func (r *testReplica) Start() error {
	r.node = rsmprotocol.NewNode(cluster.NewCluster(r.network, r.protocol, cluster.WithMemberID(r.replica.ID)), local.NewProtocol())
	rsmcounterprotocol.RegisterService(r.node)
	rsmelectionprotocol.RegisterService(r.node)
	rsmindexedmapprotocol.RegisterService(r.node)
	rsmlistprotocol.RegisterService(r.node)
	rsmlockprotocol.RegisterService(r.node)
	rsmmapprotocol.RegisterService(r.node)
	rsmsetprotocol.RegisterService(r.node)
	rsmvalueprotocol.RegisterService(r.node)
	err := r.node.Start()
	if err != nil {
		return err
	}
	return nil
}

func (r *testReplica) Stop() error {
	if r.node != nil {
		err := r.node.Stop()
		if err != nil {
			return err
		}
	}
	return nil
}
