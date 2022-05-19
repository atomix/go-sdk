// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"fmt"
	protocolapi "github.com/atomix/api/pkg/atomix/protocol"
	"github.com/atomix/runtime/pkg/cluster"
	"github.com/atomix/runtime/pkg/logging"
	"sync"
)

// Protocol is a test protocol implementation
type Protocol interface {
	// NewReplica creates a new test replica
	NewReplica(network cluster.Network, replica protocolapi.ProtocolReplica, protocol protocolapi.ProtocolConfig) Replica
	// NewClient creates a new test client
	NewClient(network cluster.Network, clientID string, protocol protocolapi.ProtocolConfig) Client
}

// Option is a test option
type Option interface {
	apply(*testOptions)
}

// testOptions is the set of all test options
type testOptions struct {
	replicas   int
	partitions int
	debug      bool
}

// WithReplicas sets the number of replicas to test
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

// WithPartitions sets the number of partitions to test
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

// WithDebugLogs sets whether to enable debug logs for the test
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
func NewTest(protocol Protocol, opts ...Option) *Test {
	network := cluster.NewLocalNetwork()
	options := testOptions{
		replicas:   1,
		partitions: 1,
	}
	for _, opt := range opts {
		opt.apply(&options)
	}
	config := newTestConfig(options)
	replicas := make([]*testReplica, len(config.Replicas))
	for i, r := range config.Replicas {
		replicas[i] = newReplica(protocol.NewReplica(network, r, config))
	}
	return &Test{
		network:  network,
		protocol: protocol,
		config:   config,
		replicas: replicas,
		debug:    options.debug,
	}
}

func newTestConfig(options testOptions) protocolapi.ProtocolConfig {
	var config protocolapi.ProtocolConfig
	var replicas []string
	var port int
	for i := 1; i <= options.replicas; i++ {
		replicaID := fmt.Sprintf("replica-%d", i)
		nodeID := fmt.Sprintf("node-%d", i)
		port = nextPort()
		config.Replicas = append(config.Replicas, protocolapi.ProtocolReplica{
			ID:      replicaID,
			NodeID:  nodeID,
			APIPort: int32(port),
		})
		replicas = append(replicas, replicaID)
	}

	for i := 1; i <= options.partitions; i++ {
		config.Partitions = append(config.Partitions, protocolapi.ProtocolPartition{
			PartitionID: uint32(i),
			Replicas:    replicas,
			APIPort:     int32(port),
		})
	}
	return config
}

// Test is an Atomix test utility
type Test struct {
	network  cluster.Network
	protocol Protocol
	config   protocolapi.ProtocolConfig
	replicas []*testReplica
	clients  []*testClient
	debug    bool
	mu       sync.Mutex
}

// Start starts the test
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

// NewClient creates a new test client
func (t *Test) NewClient(clientID string) (Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	client := newClient(clientID, t.protocol.NewClient(t.network, clientID, t.config))
	driverPort := nextPort()
	agentPort := nextPort()
	if err := client.Start(driverPort, agentPort); err != nil {
		return nil, err
	}
	t.clients = append(t.clients, client)
	return client, nil
}

// Stop stops the test
func (t *Test) Stop() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, client := range t.clients {
		if err := client.Stop(); err != nil {
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

var ports = &portAllocator{port: 5000}

func nextPort() int {
	return ports.next()
}

type portAllocator struct {
	port int
	mu   sync.Mutex
}

func (p *portAllocator) next() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	port := p.port
	p.port++
	return port
}
