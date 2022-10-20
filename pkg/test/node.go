// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	counterv1 "github.com/atomix/runtime/primitives/pkg/counter/v1"
	mapv1 "github.com/atomix/runtime/primitives/pkg/map/v1"
	"github.com/atomix/runtime/sdk/pkg/network"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/node"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	streams "github.com/atomix/runtime/sdk/pkg/stream"
	"sync"
)

func newNode(network network.Network, opts ...node.Option) *node.Node {
	node := node.NewNode(network, newProtocol(), opts...)
	counterv1.RegisterServer(node)
	mapv1.RegisterServer(node)
	return node
}

func newProtocol() node.Protocol {
	return &testProtocol{
		partitions: map[protocol.PartitionID]node.Partition{
			1: node.NewPartition(1, newExecutor()),
			2: node.NewPartition(2, newExecutor()),
			3: node.NewPartition(3, newExecutor()),
		},
	}
}

type testProtocol struct {
	partitions map[protocol.PartitionID]node.Partition
}

func (p *testProtocol) Partitions() []node.Partition {
	partitions := make([]node.Partition, 0, len(p.partitions))
	for _, partition := range p.partitions {
		partitions = append(partitions, partition)
	}
	return partitions
}

func (p *testProtocol) Partition(partitionID protocol.PartitionID) (node.Partition, bool) {
	partition, ok := p.partitions[partitionID]
	return partition, ok
}

func newExecutor() node.Executor {
	registry := statemachine.NewPrimitiveTypeRegistry()
	counterv1.RegisterStateMachine(registry)
	mapv1.RegisterStateMachine(registry)
	return &testExecutor{
		sm: statemachine.NewStateMachine(registry),
	}
}

type testExecutor struct {
	sm statemachine.StateMachine
	mu sync.RWMutex
}

func (e *testExecutor) Propose(ctx context.Context, proposal *protocol.ProposalInput, stream streams.WriteStream[*protocol.ProposalOutput]) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sm.Propose(proposal, stream)
	return nil
}

func (e *testExecutor) Query(ctx context.Context, query *protocol.QueryInput, stream streams.WriteStream[*protocol.QueryOutput]) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	e.sm.Query(query, stream)
	return nil
}
