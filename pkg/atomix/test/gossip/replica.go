// Copyright 2020-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gossip

import (
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	gossipprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip"
	gossipcounterprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/counter"
	gossipmapprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/map"
	gossipsetprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/set"
	gossipvalueprotocol "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/value"
)

func newReplica(replica protocolapi.ProtocolReplica, protocol protocolapi.ProtocolConfig) *testReplica {
	return &testReplica{
		replica:  replica,
		protocol: protocol,
	}
}

type testReplica struct {
	replica  protocolapi.ProtocolReplica
	protocol protocolapi.ProtocolConfig
	node     *gossipprotocol.Node
}

func (r *testReplica) Start() error {
	r.node = gossipprotocol.NewNode(cluster.NewCluster(r.protocol, cluster.WithMemberID(r.replica.ID)))
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
