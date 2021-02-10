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
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/util/async"
)

// newTestStorage creates a new test storage
func newTestStorage() *TestStorage {
	return &TestStorage{
		replicas:   1,
		partitions: 1,
		primitives: make(map[string]primitiveapi.PrimitiveMeta),
	}
}

// TestStorage defines a test storage configuration
type TestStorage struct {
	replicas   int
	partitions int
	primitives map[string]primitiveapi.PrimitiveMeta
	nodes      []node.Node
}

func (s *TestStorage) SetReplicas(replicas int) *TestStorage {
	s.replicas = replicas
	return s
}

func (s *TestStorage) SetPartitions(partitions int) *TestStorage {
	s.partitions = partitions
	return s
}

func (s *TestStorage) AddPrimitive(ptype primitiveapi.PrimitiveMeta) *TestStorage {
	s.primitives[ptype.Name] = ptype
	return s
}

func (s *TestStorage) getConfig() protocolapi.ProtocolConfig {
	config := protocolapi.ProtocolConfig{
		Replicas:   []protocolapi.ProtocolReplica{},
		Partitions: []protocolapi.ProtocolPartition{},
	}

	replicas := make([]string, 0, s.replicas)
	for i := 1; i <= s.replicas; i++ {
		replica := fmt.Sprintf("replica-%d", i)
		replicas = append(replicas, replica)
		config.Replicas = append(config.Replicas, protocolapi.ProtocolReplica{
			ID:           replica,
			NodeID:       fmt.Sprintf("node-%d", i),
			Host:         "localhost",
			APIPort:      int32(5600 + i),
			ProtocolPort: int32(5700 + i),
		})
	}

	for i := 1; i <= s.partitions; i++ {
		config.Partitions = append(config.Partitions, protocolapi.ProtocolPartition{
			PartitionID: uint32(i),
			Replicas:    replicas,
		})
	}
	return config
}

func (s *TestStorage) Start(f func(cluster.Cluster) node.Node) error {
	config := s.getConfig()
	protocols, err := async.ExecuteAsync(s.replicas, func(i int) (interface{}, error) {
		protocol := f(cluster.NewCluster(config, cluster.WithMemberID(fmt.Sprintf("replica-%d", i+1))))
		if err := protocol.Start(); err != nil {
			return nil, err
		}
		return protocol, nil
	})
	if err != nil {
		return err
	}
	for _, protocol := range protocols {
		s.nodes = append(s.nodes, protocol.(node.Node))
	}
	return nil
}

func (s *TestStorage) Stop() error {
	return async.IterAsync(len(s.nodes), func(i int) error {
		return s.nodes[i].Stop()
	})
}
