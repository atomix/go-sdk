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
	"context"
	"fmt"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
	"time"
)

// newTestNode creates a new test client
func newTestNode(i int, storage *TestStorage) *TestNode {
	return &TestNode{
		storage: storage,
		port:    6000 + i,
	}
}

// TestNode defines a test client configuration
type TestNode struct {
	storage *TestStorage
	proxy   node.Node
	port    int
	conn    *grpc.ClientConn
	mu      sync.RWMutex
}

func (n *TestNode) Connect(f func(cluster.Cluster) proxy.Node) (*grpc.ClientConn, error) {
	if err := n.Start(f); err != nil {
		return nil, err
	}
	return n.connect()
}

func (n *TestNode) connect() (*grpc.ClientConn, error) {
	n.mu.RLock()
	conn := n.conn
	n.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.conn != nil {
		return n.conn, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("localhost:%d", n.port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	n.conn = conn
	return conn, nil
}

func (n *TestNode) Start(f func(cluster.Cluster) proxy.Node) error {
	proxy := f(cluster.NewCluster(
		n.storage.getConfig(),
		cluster.WithMemberID("proxy"),
		cluster.WithHost("localhost"),
		cluster.WithPort(n.port)))
	if err := proxy.Start(); err != nil {
		return err
	}
	n.proxy = proxy

	conn, err := n.connect()
	if err != nil {
		return err
	}

	client := primitiveapi.NewPrimitiveManagementServiceClient(conn)
	for _, primitive := range n.storage.primitives {
		request := &primitiveapi.AddPrimitiveRequest{
			Primitive: primitive,
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		_, err := client.AddPrimitive(ctx, request)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *TestNode) Stop() error {
	if n.conn != nil {
		err := n.conn.Close()
		if err != nil {
			return err
		}
	}
	if n.proxy != nil {
		err := n.proxy.Start()
		if err != nil {
			return err
		}
	}
	return nil
}
