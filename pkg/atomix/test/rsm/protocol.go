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
)

// Protocol is a state machine test protocol implementation
var Protocol test.Protocol = &rsmProtocol{}

// rsmProtocol is a test protocol for state machine replication
type rsmProtocol struct{}

func (p *rsmProtocol) NewReplica(replica protocolapi.ProtocolReplica, protocol protocolapi.ProtocolConfig) test.Replica {
	return newReplica(replica, protocol)
}

func (p *rsmProtocol) NewClient(clientID string, protocol protocolapi.ProtocolConfig) test.Client {
	return newClient(clientID, protocol)
}
