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
	"fmt"
	controllerapi "github.com/atomix/api/proto/atomix/controller"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-client/pkg/client/util/net"
	"google.golang.org/grpc"
	"sort"
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

	client := &Client{
		conn:             conn,
		options:          options,
		databases:        make(map[string]*Database),
		partitionGroups:  make(map[string]*PartitionGroup),
		membershipGroups: make(map[string]*MembershipGroup),
	}
	client.cluster = &Cluster{
		client:   client,
		watchers: make([]chan<- ClusterMembership, 0),
	}

	if options.joinTimeout != nil {
		ctx, cancel := context.WithTimeout(context.Background(), *options.joinTimeout)
		err = client.cluster.join(ctx)
		cancel()
	} else {
		err = client.cluster.join(context.Background())
	}
	if err != nil {
		return nil, err
	}
	return client, nil
}

// NewClient returns a new Atomix client
// Deprected: use New instead
func NewClient(address string, opts ...Option) (*Client, error) {
	return New(address, opts...)
}

// Client is an Atomix client
type Client struct {
	options          clientOptions
	conn             *grpc.ClientConn
	databases        map[string]*Database
	cluster          *Cluster
	partitionGroups  map[string]*PartitionGroup
	membershipGroups map[string]*MembershipGroup
	mu               sync.RWMutex
}

// Cluster returns the client membership API
func (c *Client) Cluster() *Cluster {
	return c.cluster
}

// GetDatabases returns a list of all databases in the client's namespace
func (c *Client) GetDatabases(ctx context.Context) ([]*Database, error) {
	client := controllerapi.NewControllerServiceClient(c.conn)
	request := &controllerapi.GetDatabasesRequest{
		ID: &controllerapi.DatabaseId{
			Namespace: c.options.namespace,
		},
	}

	response, err := client.GetDatabases(ctx, request)
	if err != nil {
		return nil, err
	}

	databases := make([]*Database, len(response.Databases))
	for i, databaseProto := range response.Databases {
		database, err := c.newDatabase(ctx, databaseProto)
		if err != nil {
			return nil, err
		}
		databases[i] = database
	}
	return databases, nil
}

// GetDatabase gets a database client by name from the client's namespace
func (c *Client) GetDatabase(ctx context.Context, name string) (*Database, error) {
	client := controllerapi.NewControllerServiceClient(c.conn)
	request := &controllerapi.GetDatabasesRequest{
		ID: &controllerapi.DatabaseId{
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
	return c.newDatabase(ctx, response.Databases[0])
}

func (c *Client) newDatabase(ctx context.Context, databaseProto *controllerapi.Database) (*Database, error) {
	// Ensure the partitions are sorted in case the controller sent them out of order.
	partitionProtos := databaseProto.Partitions
	sort.Slice(partitionProtos, func(i, j int) bool {
		return partitionProtos[i].PartitionID < partitionProtos[j].PartitionID
	})

	// Iterate through the partitions and create gRPC client connections for each partition.
	partitions := make([]primitive.Partition, len(databaseProto.Partitions))
	for i, partitionProto := range partitionProtos {
		ep := partitionProto.Endpoints[0]
		partitions[i] = primitive.Partition{
			ID:      int(partitionProto.PartitionID),
			Address: net.Address(fmt.Sprintf("%s:%d", ep.Host, ep.Port)),
		}
	}

	// Iterate through partitions and open sessions
	sessions := make([]*primitive.Session, len(partitions))
	for i, partition := range partitions {
		session, err := primitive.NewSession(ctx, partition, primitive.WithSessionTimeout(c.options.sessionTimeout))
		if err != nil {
			return nil, err
		}
		sessions[i] = session
	}

	return &Database{
		Namespace: databaseProto.ID.Namespace,
		Name:      databaseProto.ID.Name,
		scope:     c.options.scope,
		sessions:  sessions,
	}, nil
}

// GetPartitionGroup gets a partition group by name from the client's namespace
func (c *Client) GetPartitionGroup(ctx context.Context, name string, opts ...PartitionGroupOption) (*PartitionGroup, error) {
	c.mu.RLock()
	group, ok := c.partitionGroups[name]
	c.mu.RUnlock()
	if ok {
		return group, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	group, ok = c.partitionGroups[name]
	if ok {
		return group, nil
	}

	options := applyPartitionGroupOptions(opts...)

	group = &PartitionGroup{
		Name:      name,
		Namespace: c.options.namespace,
		client:    c,
		options:   options,
	}
	var err error
	if c.options.joinTimeout != nil {
		ctx, cancel := context.WithTimeout(context.Background(), *c.options.joinTimeout)
		err = group.join(ctx)
		cancel()
	} else {
		err = group.join(context.Background())
	}
	if err != nil {
		return nil, err
	}
	c.partitionGroups[name] = group
	return group, nil
}

// GetMembershipGroup gets a membership group by name from the client's namespace
func (c *Client) GetMembershipGroup(ctx context.Context, name string) (*MembershipGroup, error) {
	c.mu.RLock()
	group, ok := c.membershipGroups[name]
	c.mu.RUnlock()
	if ok {
		return group, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	group, ok = c.membershipGroups[name]
	if ok {
		return group, nil
	}

	group = &MembershipGroup{
		Name:      name,
		Namespace: c.options.namespace,
		client:    c,
	}
	var err error
	if c.options.joinTimeout != nil {
		ctx, cancel := context.WithTimeout(context.Background(), *c.options.joinTimeout)
		err = group.join(ctx)
		cancel()
	} else {
		err = group.join(context.Background())
	}
	if err != nil {
		return nil, err
	}
	c.membershipGroups[name] = group
	return group, nil
}

// Close closes the client
func (c *Client) Close() error {
	if c.options.joinTimeout != nil {
		ctx, cancel := context.WithTimeout(context.Background(), *c.options.joinTimeout)
		_ = c.cluster.leave(ctx)
		cancel()
	} else {
		_ = c.cluster.leave(context.Background())
	}
	if err := c.conn.Close(); err != nil {
		return err
	}
	return nil
}
