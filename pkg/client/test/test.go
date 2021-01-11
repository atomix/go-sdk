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

package test

import (
	"fmt"
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	rsmprotocol "github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	rsmproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	"github.com/atomix/go-local/pkg/atomix/local"
	"google.golang.org/grpc"
)

// New creates a new test
func New() *Test {
	return &Test{
		partitions:      1,
		sessions:        1,
		registerProxy:   func(node *rsmproxy.Node) {},
		registerStorage: func(node *rsmprotocol.Node) {},
	}
}

// Test is a test context
type Test struct {
	partitions      int
	sessions        int
	registerProxy   func(node *rsmproxy.Node)
	proxies         []*rsmproxy.Node
	registerStorage func(node *rsmprotocol.Node)
	storage         *rsmprotocol.Node
	conns           []*grpc.ClientConn
}

func (t *Test) SetPartitions(partitions int) *Test {
	t.partitions = partitions
	return t
}

func (t *Test) SetSessions(sessions int) *Test {
	t.sessions = sessions
	return t
}

func (t *Test) SetProxy(f func(node *rsmproxy.Node)) *Test {
	t.registerProxy = f
	return t
}

func (t *Test) SetStorage(f func(node *rsmprotocol.Node)) *Test {
	t.registerStorage = f
	return t
}

func (t *Test) Start() ([]*grpc.ClientConn, error) {
	config := protocolapi.ProtocolConfig{
		Replicas: []protocolapi.ProtocolReplica{
			{
				ID:           "replica-1",
				NodeID:       "node-1",
				Host:         "localhost",
				APIPort:      5678,
				ProtocolPort: 5679,
			},
		},
		Partitions: []protocolapi.ProtocolPartition{},
	}

	for i := 1; i <= t.partitions; i++ {
		config.Partitions = append(config.Partitions, protocolapi.ProtocolPartition{
			PartitionID: uint32(i),
			Replicas:    []string{"replica-1"},
		})
	}

	t.storage = rsmprotocol.NewNode(cluster.NewCluster(config, cluster.WithMemberID("replica-1")), local.NewProtocol())
	t.registerStorage(t.storage)
	if err := t.storage.Start(); err != nil {
		return nil, err
	}

	t.proxies = make([]*rsmproxy.Node, 0, t.sessions)
	t.conns = make([]*grpc.ClientConn, 0, t.sessions)
	for i := 1; i <= t.sessions; i++ {
		port := 5700 + i
		proxy := rsmproxy.NewNode(cluster.NewCluster(config, cluster.WithMemberID("proxy-1"), cluster.WithHost("localhost"), cluster.WithPort(port)))
		t.registerProxy(proxy)
		if err := proxy.Start(); err != nil {
			return nil, err
		}
		t.proxies = append(t.proxies, proxy)

		conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		t.conns = append(t.conns, conn)
	}
	return t.conns, nil
}

func (t *Test) Stop() error {
	for _, conn := range t.conns {
		conn.Close()
	}
	for _, proxy := range t.proxies {
		proxy.Stop()
	}
	return t.storage.Stop()
}
