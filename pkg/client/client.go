// Copyright 2019-present Open Networking Foundation.
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

package client

import (
	"context"
	"errors"
	databaseapi "github.com/atomix/api/proto/atomix/database"
	"github.com/atomix/go-client/pkg/client/cluster"
	"github.com/atomix/go-client/pkg/client/database"
	"github.com/atomix/go-client/pkg/client/membership"
	"github.com/atomix/go-client/pkg/client/partition"
	"github.com/atomix/go-client/pkg/client/protocol"
	"google.golang.org/grpc"
	"sync"
)

// New returns a new Atomix client
func New(address string, opts ...Option) (*Client, error) {
	options := applyOptions(opts...)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	clusterOpts := []cluster.Option{
		cluster.WithNamespace(options.namespace),
		cluster.WithScope(options.scope),
		cluster.WithMemberID(options.memberID),
		cluster.WithPeerHost(options.peerHost),
		cluster.WithPeerPort(options.peerPort),
	}
	if options.joinTimeout != nil {
		clusterOpts = append(clusterOpts, cluster.WithJoinTimeout(*options.joinTimeout))
	}

	cluster, err := cluster.New(address, clusterOpts...)
	if err != nil {
		return nil, err
	}

	client := &Client{
		address:          address,
		conn:             conn,
		cluster:          cluster,
		options:          options,
		databases:        make(map[string]*database.Database),
		partitionGroups:  make(map[string]*partition.Group),
		membershipGroups: make(map[string]*membership.Group),
	}
	return client, nil
}

// Client is an Atomix client
type Client struct {
	address          string
	options          clientOptions
	conn             *grpc.ClientConn
	databases        map[string]*database.Database
	cluster          *cluster.Cluster
	partitionGroups  map[string]*partition.Group
	membershipGroups map[string]*membership.Group
	mu               sync.RWMutex
}

// Cluster returns the client membership API
func (c *Client) Cluster() *cluster.Cluster {
	return c.cluster
}

// GetDatabases returns a list of all databases in the client's namespace
func (c *Client) GetDatabases(ctx context.Context, opts ...database.Option) ([]*database.Database, error) {
	client := databaseapi.NewDatabaseServiceClient(c.conn)
	request := &databaseapi.GetDatabasesRequest{
		ID: &databaseapi.DatabaseId{
			Namespace: c.options.namespace,
		},
	}

	response, err := client.GetDatabases(ctx, request)
	if err != nil {
		return nil, err
	}

	databases := make([]*database.Database, len(response.Databases))
	for i, databaseProto := range response.Databases {
		database, err := database.New(ctx, protocol.New(c.conn, c.options.namespace, databaseProto.ID.Name, c.options.scope), databaseProto, opts...)
		if err != nil {
			return nil, err
		}
		databases[i] = database
	}
	return databases, nil
}

// GetDatabase gets a database client by name from the client's namespace
func (c *Client) GetDatabase(ctx context.Context, name string, opts ...database.Option) (*database.Database, error) {
	client := databaseapi.NewDatabaseServiceClient(c.conn)
	request := &databaseapi.GetDatabasesRequest{
		ID: &databaseapi.DatabaseId{
			Name:      name,
			Namespace: c.options.namespace,
		},
	}

	response, err := client.GetDatabases(ctx, request)
	if err != nil {
		return nil, err
	}

	if len(response.Databases) == 0 {
		return nil, errors.New("unknown database " + name)
	} else if len(response.Databases) > 1 {
		return nil, errors.New("database " + name + " is ambiguous")
	}
	return database.New(ctx, protocol.New(c.conn, c.options.namespace, name, c.options.scope), response.Databases[0], opts...)
}

// GetMembershipGroup gets a membership group by name from the client's namespace
func (c *Client) GetMembershipGroup(ctx context.Context, name string, opts ...membership.Option) (*membership.Group, error) {
	c.mu.RLock()
	membershipGroup, ok := c.membershipGroups[name]
	c.mu.RUnlock()
	if ok {
		return membershipGroup, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	membershipGroup, ok = c.membershipGroups[name]
	if ok {
		return membershipGroup, nil
	}

	membershipGroup, err := membership.NewGroup(ctx, c.conn, c.cluster, protocol.New(c.conn, c.options.namespace, name, c.options.scope), opts...)
	if err != nil {
		return nil, err
	}
	c.membershipGroups[name] = membershipGroup
	return membershipGroup, nil
}

// GetPartitionGroup gets a partition group by name from the client's namespace
func (c *Client) GetPartitionGroup(ctx context.Context, name string, opts ...partition.Option) (*partition.Group, error) {
	c.mu.RLock()
	partitionGroup, ok := c.partitionGroups[name]
	c.mu.RUnlock()
	if ok {
		return partitionGroup, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	partitionGroup, ok = c.partitionGroups[name]
	if ok {
		return partitionGroup, nil
	}

	partitionGroup, err := partition.NewGroup(ctx, c.conn, c.cluster, protocol.New(c.conn, c.options.namespace, name, c.options.scope), opts...)
	if err != nil {
		return nil, err
	}
	c.partitionGroups[name] = partitionGroup
	return partitionGroup, nil
}

// Close closes the client
func (c *Client) Close() error {
	_ = c.cluster.Close()
	if err := c.conn.Close(); err != nil {
		return err
	}
	return nil
}
