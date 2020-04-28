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
	"github.com/atomix/go-client/pkg/client/counter"
	"github.com/atomix/go-client/pkg/client/election"
	"github.com/atomix/go-client/pkg/client/indexedmap"
	"github.com/atomix/go-client/pkg/client/leader"
	"github.com/atomix/go-client/pkg/client/list"
	"github.com/atomix/go-client/pkg/client/lock"
	"github.com/atomix/go-client/pkg/client/log"
	"github.com/atomix/go-client/pkg/client/map"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-client/pkg/client/set"
	"github.com/atomix/go-client/pkg/client/util"
	"github.com/atomix/go-client/pkg/client/util/net"
	"github.com/atomix/go-client/pkg/client/value"
	"google.golang.org/grpc"
	"io"
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

	if options.joinTimeout != nil {
		ctx, cancel := context.WithTimeout(context.Background(), *options.joinTimeout)
		err = client.join(ctx)
		cancel()
	} else {
		err = client.join(context.Background())
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
	partitionGroups  map[string]*PartitionGroup
	membershipGroups map[string]*MembershipGroup
	mu               sync.RWMutex
}

// join joins the cluster
func (c *Client) join(ctx context.Context) error {
	if c.options.memberID == "" {
		return nil
	}

	client := controllerapi.NewClusterServiceClient(c.conn)
	request := &controllerapi.JoinClusterRequest{
		Member: controllerapi.Member{
			ID: controllerapi.MemberId{
				Namespace: c.options.namespace,
				Name:      c.options.memberID,
			},
			Host: c.options.peerHost,
			Port: int32(c.options.peerPort),
		},
		GroupID: controllerapi.MembershipGroupId{
			Namespace: c.options.namespace,
			Name:      c.options.scope,
		},
	}
	stream, err := client.JoinCluster(ctx, request)
	if err != nil {
		return err
	}
	go func() {
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err == nil {
				// TODO: Update membership
			}
		}
	}()
	return nil
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
	if c.options.memberID == "" {
		return nil, errors.New("cannot join partition group: member not configured")
	}

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
		Name:         name,
		Namespace:    c.options.namespace,
		client:       c,
		groupOptions: options,
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
	if c.options.memberID == "" {
		return nil, errors.New("cannot join partition group: member not configured")
	}

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
	if err := c.conn.Close(); err != nil {
		return err
	}
	return nil
}

// Database manages the primitives in a set of partitions
type Database struct {
	Namespace string
	Name      string

	scope    string
	sessions []*primitive.Session
}

// GetPrimitives gets a list of primitives in the database
func (d *Database) GetPrimitives(ctx context.Context, opts ...primitive.MetadataOption) ([]primitive.Metadata, error) {
	dupPrimitives, err := util.ExecuteAsync(len(d.sessions), func(i int) (interface{}, error) {
		return d.sessions[i].GetPrimitives(ctx, opts...)
	})
	if err != nil {
		return nil, err
	}

	dedupPrimitives := make(map[string]primitive.Metadata)
	for _, partPrimitives := range dupPrimitives {
		for _, partPrimitive := range partPrimitives.([]primitive.Metadata) {
			dedupPrimitives[partPrimitive.Name.String()] = partPrimitive
		}
	}

	primitives := make([]primitive.Metadata, 0, len(dedupPrimitives))
	for _, primitive := range dedupPrimitives {
		primitives = append(primitives, primitive)
	}
	return primitives, nil
}

// GetCounter gets or creates a Counter with the given name
func (d *Database) GetCounter(ctx context.Context, name string) (counter.Counter, error) {
	return counter.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions)
}

// GetElection gets or creates an Election with the given name
func (d *Database) GetElection(ctx context.Context, name string, opts ...election.Option) (election.Election, error) {
	return election.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions)
}

// GetIndexedMap gets or creates a Map with the given name
func (d *Database) GetIndexedMap(ctx context.Context, name string) (indexedmap.IndexedMap, error) {
	return indexedmap.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions)
}

// GetLeaderLatch gets or creates a LeaderLatch with the given name
func (d *Database) GetLeaderLatch(ctx context.Context, name string, opts ...leader.Option) (leader.Latch, error) {
	return leader.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions)
}

// GetList gets or creates a List with the given name
func (d *Database) GetList(ctx context.Context, name string) (list.List, error) {
	return list.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions)
}

// GetLock gets or creates a Lock with the given name
func (d *Database) GetLock(ctx context.Context, name string) (lock.Lock, error) {
	return lock.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions)
}

// GetLog gets or creates a Log with the given name
func (d *Database) GetLog(ctx context.Context, name string) (log.Log, error) {
	return log.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions)
}

// GetMap gets or creates a Map with the given name
func (d *Database) GetMap(ctx context.Context, name string, opts ..._map.Option) (_map.Map, error) {
	return _map.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions, opts...)
}

// GetSet gets or creates a Set with the given name
func (d *Database) GetSet(ctx context.Context, name string) (set.Set, error) {
	return set.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions)
}

// GetValue gets or creates a Value with the given name
func (d *Database) GetValue(ctx context.Context, name string) (value.Value, error) {
	return value.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions)
}

// PartitionGroup manages the primitives in a partition group
type PartitionGroup struct {
	Namespace    string
	Name         string
	client       *Client
	groupOptions partitionGroupOptions
}

// join joins the partition group
func (g *PartitionGroup) join(ctx context.Context) error {
	if g.client.options.memberID == "" {
		return nil
	}

	client := controllerapi.NewPartitionGroupServiceClient(g.client.conn)
	request := &controllerapi.JoinPartitionGroupRequest{
		MemberID: controllerapi.MemberId{
			Namespace: g.client.options.namespace,
			Name:      g.client.options.memberID,
		},
		GroupID: controllerapi.PartitionGroupId{
			Namespace: g.Namespace,
			Name:      g.Name,
		},
		Partitions:        uint32(g.groupOptions.partitions),
		ReplicationFactor: uint32(g.groupOptions.replicationFactor),
	}
	stream, err := client.JoinPartitionGroup(ctx, request)
	if err != nil {
		return err
	}
	go func() {
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err == nil {
				// TODO: Update membership
			}
		}
	}()
	return nil
}

// MembershipGroup manages the primitives in a membership group
type MembershipGroup struct {
	Namespace string
	Name      string
	client    *Client
}

// join joins the membership group
func (g *MembershipGroup) join(ctx context.Context) error {
	if g.client.options.memberID == "" {
		return nil
	}

	client := controllerapi.NewMembershipGroupServiceClient(g.client.conn)
	request := &controllerapi.JoinMembershipGroupRequest{
		MemberID: controllerapi.MemberId{
			Namespace: g.client.options.namespace,
			Name:      g.client.options.memberID,
		},
		GroupID: controllerapi.MembershipGroupId{
			Namespace: g.Namespace,
			Name:      g.Name,
		},
	}
	stream, err := client.JoinMembershipGroup(ctx, request)
	if err != nil {
		return err
	}
	go func() {
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err == nil {
				// TODO: Update membership
			}
		}
	}()
	return nil
}
