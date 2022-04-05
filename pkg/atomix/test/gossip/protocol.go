// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	test "github.com/atomix/atomix-go-client/pkg/atomix/test"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/proxy/gossip"
)

type gossipOptions struct {
	gossip.GossipConfig
}

// Option is a gossip protocol option
type Option interface {
	apply(*gossipOptions)
}

type gossipOption struct {
	applyFunc func(*gossipOptions)
}

func (g gossipOption) apply(options *gossipOptions) {
	g.applyFunc(options)
}

func newOption(f func(options *gossipOptions)) Option {
	return &gossipOption{
		applyFunc: f,
	}
}

// WithLogicalClock configures the gossip protocol to use a logical clock for primitives
func WithLogicalClock() Option {
	return newOption(func(options *gossipOptions) {
		options.Clock = &gossip.GossipClock{
			Clock: &gossip.GossipClock_Logical{
				Logical: &gossip.LogicalClock{},
			},
		}
	})
}

// WithPhysicalClock configures the gossip protocol to use a physical clock for primitives
func WithPhysicalClock() Option {
	return newOption(func(options *gossipOptions) {
		options.Clock = &gossip.GossipClock{
			Clock: &gossip.GossipClock_Physical{
				Physical: &gossip.PhysicalClock{},
			},
		}
	})
}

// NewProtocol creates a new gossip test protocol
func NewProtocol(opts ...Option) test.Protocol {
	options := gossipOptions{
		GossipConfig: gossip.GossipConfig{
			Clock: &gossip.GossipClock{
				Clock: &gossip.GossipClock_Logical{
					Logical: &gossip.LogicalClock{},
				},
			},
		},
	}
	for _, opt := range opts {
		opt.apply(&options)
	}
	return &gossipProtocol{
		options: options,
	}
}

// gossipProtocol is a test protocol for gossip replication
type gossipProtocol struct {
	options gossipOptions
}

func (p *gossipProtocol) NewReplica(network cluster.Network, replica protocolapi.ProtocolReplica, protocol protocolapi.ProtocolConfig) test.Replica {
	return newReplica(network, replica, protocol)
}

func (p *gossipProtocol) NewClient(network cluster.Network, clientID string, protocol protocolapi.ProtocolConfig) test.Client {
	return newClient(network, clientID, protocol, p.options)
}
