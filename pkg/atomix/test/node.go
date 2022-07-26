// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"fmt"
	multiraftv1 "github.com/atomix/multi-raft-storage/api/atomix/multiraft/v1"
	"github.com/atomix/multi-raft-storage/node/pkg/node"
)

func newNode(test *Cluster, id multiraftv1.NodeID) *Node {
	return &Node{
		test: test,
		id:   id,
	}
}

type Node struct {
	test *Cluster
	id   multiraftv1.NodeID
	node *node.MultiRaftNode
}

func (n *Node) Start() error {
	n.node = node.New(n.test.network,
		node.WithHost("localhost"),
		node.WithPort(5680+int(n.id)),
		node.WithConfig(multiraftv1.NodeConfig{
			NodeID: n.id,
			Host:   "localhost",
			Port:   5690 + int32(n.id),
			MultiRaftConfig: multiraftv1.MultiRaftConfig{
				DataDir: fmt.Sprintf("test-data/%d", n.id),
			},
		}))
	if err := n.node.Start(); err != nil {
		return err
	}

	conn, err := n.test.connect(context.Background(), fmt.Sprintf("%s:%d", n.node.Host, n.node.Port))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := multiraftv1.NewNodeClient(conn)
	request := &multiraftv1.BootstrapRequest{
		Cluster: n.test.config,
	}
	_, err = client.Bootstrap(context.Background(), request)
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) Stop() error {
	if n.node != nil {
		return n.node.Stop()
	}
	return nil
}
