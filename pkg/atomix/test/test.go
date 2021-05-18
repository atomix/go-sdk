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
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-client/pkg/atomix"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"sync"
)

type Option interface {
	apply(*testOptions)
}

type testOptions struct {
	replicas   int
	partitions int
	debug      bool
}

func WithReplicas(replicas int) Option {
	return replicasOption{
		replicas: replicas,
	}
}

type replicasOption struct {
	replicas int
}

func (o replicasOption) apply(options *testOptions) {
	options.replicas = o.replicas
}

func WithPartitions(partitions int) Option {
	return partitionsOption{
		partitions: partitions,
	}
}

type partitionsOption struct {
	partitions int
}

func (o partitionsOption) apply(options *testOptions) {
	options.partitions = o.partitions
}

func WithDebugLogs() Option {
	return debugOption{
		debug: true,
	}
}

type debugOption struct {
	debug bool
}

func (o debugOption) apply(options *testOptions) {
	options.debug = o.debug
}

// NewTest creates a new Atomix test
func NewTest(opts ...Option) *Test {
	options := testOptions{
		replicas:   1,
		partitions: 1,
	}
	for _, opt := range opts {
		opt.apply(&options)
	}
	config := newTestConfig(options)
	replicas := make([]*Replica, len(config.Replicas))
	for i, r := range config.Replicas {
		replicas[i] = newReplica(r, config)
	}
	return &Test{
		config:     config,
		replicas:   replicas,
		driverPort: 5252,
		agentPort:  5353,
		debug:      options.debug,
	}
}

func newTestConfig(options testOptions) protocolapi.ProtocolConfig {
	var config protocolapi.ProtocolConfig
	var replicaPort int32 = 7000
	var replicas []string
	for i := 1; i <= options.replicas; i++ {
		replicaID := fmt.Sprintf("replica-%d", i)
		nodeID := fmt.Sprintf("node-%d", i)
		config.Replicas = append(config.Replicas, protocolapi.ProtocolReplica{
			ID:      replicaID,
			NodeID:  nodeID,
			Host:    "localhost",
			APIPort: replicaPort,
		})
		replicas = append(replicas, replicaID)
		replicaPort++
	}

	for i := 1; i <= options.partitions; i++ {
		config.Partitions = append(config.Partitions, protocolapi.ProtocolPartition{
			PartitionID: uint32(i),
			Replicas:    replicas,
		})
	}
	return config
}

// Test is an Atomix test utility
type Test struct {
	config     protocolapi.ProtocolConfig
	replicas   []*Replica
	clients    []*Client
	driverPort int
	agentPort  int
	debug      bool
	mu         sync.Mutex
}

func (t *Test) Start() error {
	if t.debug {
		logging.SetLevel(logging.DebugLevel)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, replica := range t.replicas {
		if err := replica.Start(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Test) NewClient(clientID string) (atomix.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	client := newClient(clientID, t.config)
	if err := client.connect(t.driverPort, t.agentPort); err != nil {
		return nil, err
	}
	t.driverPort++
	t.agentPort++
	t.clients = append(t.clients, client)
	return client, nil
}

func (t *Test) Stop() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, client := range t.clients {
		if err := client.Close(); err != nil {
			return err
		}
	}
	for _, replica := range t.replicas {
		if err := replica.Stop(); err != nil {
			return err
		}
	}
	return nil
}
