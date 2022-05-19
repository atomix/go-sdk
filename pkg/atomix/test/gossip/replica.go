// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	protocolapi "github.com/atomix/api/pkg/atomix/protocol"
	"github.com/atomix/runtime/pkg/cluster"
	gossipprotocol "github.com/atomix/runtime/pkg/storage/protocol/gossip"
	gossipcounterprotocol "github.com/atomix/runtime/pkg/storage/protocol/gossip/counter"
	gossipmapprotocol "github.com/atomix/runtime/pkg/storage/protocol/gossip/map"
	gossipsetprotocol "github.com/atomix/runtime/pkg/storage/protocol/gossip/set"
	gossipvalueprotocol "github.com/atomix/runtime/pkg/storage/protocol/gossip/value"
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
	node     *gossipprotocol.Node
}

func (r *testReplica) Start() error {
	r.node = gossipprotocol.NewNode(cluster.NewCluster(r.network, r.protocol, cluster.WithMemberID(r.replica.ID)))
	gossipcounterprotocol.RegisterService(r.node)
	gossipmapprotocol.RegisterService(r.node)
	gossipsetprotocol.RegisterService(r.node)
	gossipvalueprotocol.RegisterService(r.node)

	gossipcounterprotocol.RegisterServer(r.node)
	gossipmapprotocol.RegisterServer(r.node)
	gossipsetprotocol.RegisterServer(r.node)
	gossipvalueprotocol.RegisterServer(r.node)

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
