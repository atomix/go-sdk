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
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip"
	gossipcounterprotocol "github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/counter"
	gossipmapprotocol "github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/map"
	gossipsetprotocol "github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/set"
	gossipvalueprotocol "github.com/atomix/go-framework/pkg/atomix/storage/protocol/gossip/value"
	atime "github.com/atomix/go-framework/pkg/atomix/time"
)

func NewCluster(config protocolapi.ProtocolConfig) *Cluster {
	return &Cluster{
		config:   config,
		nodes:    make(map[string]*Node),
		nextPort: 55680,
	}
}

type Cluster struct {
	config   protocolapi.ProtocolConfig
	protocol []*gossip.Node
	nodes    map[string]*Node
	nextPort int
}

func (c *Cluster) Node(name string) *Node {
	node, ok := c.nodes[name]
	if !ok {
		driverPort := c.nextPort
		c.nextPort++
		agentPort := c.nextPort
		c.nextPort++
		node = &Node{
			brokerPort: driverPort,
			agentPort:  agentPort,
		}
		c.nodes[name] = node
	}
	return node
}

func (c *Cluster) Start() error {
	for _, replica := range c.config.Replicas {
		node := gossip.NewNode(cluster.NewCluster(c.config, cluster.WithMemberID(replica.ID)), atime.LogicalScheme)
		gossipcounterprotocol.RegisterService(node)
		gossipmapprotocol.RegisterService(node)
		gossipsetprotocol.RegisterService(node)
		gossipvalueprotocol.RegisterService(node)
		gossipcounterprotocol.RegisterServer(node)
		gossipmapprotocol.RegisterServer(node)
		gossipsetprotocol.RegisterServer(node)
		gossipvalueprotocol.RegisterServer(node)
		if err := node.Start(); err != nil {
			return err
		}
		c.protocol = append(c.protocol, node)
	}
	return nil
}

func (c *Cluster) Stop() error {
	for _, node := range c.protocol {
		if err := node.Stop(); err != nil {
			return err
		}
	}
	return nil
}
