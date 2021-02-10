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
	"github.com/atomix/go-framework/pkg/atomix/util/async"
)

// NewTestCluster creates a new test cluster
func NewTestCluster() *TestCluster {
	return &TestCluster{
		storage: newTestStorage(),
		nodes:   make(map[int]*TestNode),
	}
}

// TestCluster is a test context
type TestCluster struct {
	storage *TestStorage
	nodes   map[int]*TestNode
}

func (c *TestCluster) Storage() *TestStorage {
	return c.storage
}

func (c *TestCluster) Node(i int) *TestNode {
	client, ok := c.nodes[i]
	if !ok {
		client = newTestNode(i, c.storage)
		c.nodes[i] = client
	}
	return client
}

func (c *TestCluster) Stop() error {
	nodes := make([]*TestNode, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	err := async.IterAsync(len(nodes), func(i int) error {
		return nodes[i].Stop()
	})
	if err != nil {
		return err
	}
	return c.storage.Stop()
}
