// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	protocolapi "github.com/atomix/api/pkg/atomix/protocol"
	test "github.com/atomix/go-client/pkg/atomix/test"
	"github.com/atomix/runtime/pkg/cluster"
)

type rsmOptions struct{}

// Option is a state machine protocol option
type Option interface {
	apply(*rsmOptions)
}

// NewProtocol creates a new state machine test protocol
func NewProtocol(opts ...Option) test.Protocol {
	options := rsmOptions{}
	for _, opt := range opts {
		opt.apply(&options)
	}
	return &rsmProtocol{
		options: options,
	}
}

// rsmProtocol is a test protocol for state machine replication
type rsmProtocol struct {
	options rsmOptions
}

func (p *rsmProtocol) NewReplica(network cluster.Network, replica protocolapi.ProtocolReplica, protocol protocolapi.ProtocolConfig) test.Replica {
	return newReplica(network, replica, protocol)
}

func (p *rsmProtocol) NewClient(network cluster.Network, clientID string, protocol protocolapi.ProtocolConfig) test.Client {
	return newClient(network, clientID, protocol)
}
