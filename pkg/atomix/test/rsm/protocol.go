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

package rsm

import (
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	test "github.com/atomix/atomix-go-client/pkg/atomix/test"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
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
