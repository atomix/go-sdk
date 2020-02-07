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
	"github.com/atomix/go-client/pkg/client/map"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-client/pkg/client/set"
	"github.com/atomix/go-client/pkg/client/util"
	"github.com/atomix/go-client/pkg/client/util/net"
	"github.com/atomix/go-client/pkg/client/value"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
	"sort"
	"time"
)

// NewClient returns a new Atomix client
func NewClient(address string, opts ...Option) (*Client, error) {
	options := applyOptions(opts...)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:           conn,
		scope:          options.scope,
		namespace:      options.namespace,
		sessionTimeout: options.sessionTimeout,
		conns:          []*grpc.ClientConn{},
	}, nil
}

// Client is an Atomix client
type Client struct {
	scope          string
	namespace      string
	sessionTimeout time.Duration
	conn           *grpc.ClientConn
	conns          []*grpc.ClientConn
}

// GetDatabases returns a list of all databases in the client's namespace
func (c *Client) GetDatabases(ctx context.Context) ([]*Database, error) {
	client := controllerapi.NewControllerServiceClient(c.conn)
	request := &controllerapi.GetDatabasesRequest{
		ID: &controllerapi.DatabaseId{
			Namespace: c.namespace,
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
			Namespace: c.namespace,
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
		session, err := primitive.NewSession(ctx, partition, primitive.WithSessionTimeout(c.sessionTimeout))
		if err != nil {
			return nil, err
		}
		sessions[i] = session
	}

	return &Database{
		Namespace: databaseProto.ID.Namespace,
		Name:      databaseProto.ID.Name,
		scope:     c.scope,
		sessions:  sessions,
	}, nil
}

// CreateGroup creates a new partition group
// Deprecated: Groups have been replaced with Databases and can only be modified by the controller
//nolint:staticcheck
func (c *Client) CreateGroup(ctx context.Context, name string, partitions int, partitionSize int, protocol proto.Message) (*PartitionGroup, error) {
	client := controllerapi.NewControllerServiceClient(c.conn)

	typeURL := "type.googleapis.com/" + proto.MessageName(protocol)
	bytes, err := proto.Marshal(protocol)
	if err != nil {
		return nil, err
	}

	request := &controllerapi.CreatePartitionGroupRequest{
		ID: &controllerapi.PartitionGroupId{
			Name:      name,
			Namespace: c.namespace,
		},
		Spec: &controllerapi.PartitionGroupSpec{
			Partitions:    uint32(partitions),
			PartitionSize: uint32(partitionSize),
			Protocol: &types.Any{
				TypeUrl: typeURL,
				Value:   bytes,
			},
		},
	}

	_, err = client.CreatePartitionGroup(ctx, request)
	if err != nil {
		return nil, err
	}
	return c.GetGroup(ctx, name)
}

// GetGroups returns a list of all partition group in the client's namespace
// Deprecated: Groups have been replaced with Databases. Use GetDatabases instead.
//nolint:staticcheck
func (c *Client) GetGroups(ctx context.Context) ([]*PartitionGroup, error) {
	client := controllerapi.NewControllerServiceClient(c.conn)
	request := &controllerapi.GetPartitionGroupsRequest{
		ID: &controllerapi.PartitionGroupId{
			Namespace: c.namespace,
		},
	}

	response, err := client.GetPartitionGroups(ctx, request)
	if err != nil {
		return nil, err
	}

	groups := make([]*PartitionGroup, len(response.Groups))
	for i, groupProto := range response.Groups {
		group, err := c.newGroup(ctx, groupProto)
		if err != nil {
			return nil, err
		}
		groups[i] = group
	}
	return groups, nil
}

// GetGroup returns a partition group primitive client
// Deprecated: Groups have been replaced with Databases. Use GetDatabase instead.
//nolint:staticcheck
func (c *Client) GetGroup(ctx context.Context, name string) (*PartitionGroup, error) {
	client := controllerapi.NewControllerServiceClient(c.conn)
	request := &controllerapi.GetPartitionGroupsRequest{
		ID: &controllerapi.PartitionGroupId{
			Name:      name,
			Namespace: c.namespace,
		},
	}

	response, err := client.GetPartitionGroups(ctx, request)
	if err != nil {
		return nil, err
	}

	if len(response.Groups) == 0 {
		return nil, errors.New("unknown partition group " + name)
	} else if len(response.Groups) > 1 {
		return nil, errors.New("partition group " + name + " is ambiguous")
	}
	return c.newGroup(ctx, response.Groups[0])
}

//nolint:staticcheck
func (c *Client) newGroup(ctx context.Context, groupProto *controllerapi.PartitionGroup) (*PartitionGroup, error) {
	// Ensure the partitions are sorted in case the controller sent them out of order.
	partitionProtos := groupProto.Partitions
	sort.Slice(partitionProtos, func(i, j int) bool {
		return partitionProtos[i].PartitionID < partitionProtos[j].PartitionID
	})

	// Iterate through the partitions and create gRPC client connections for each partition.
	partitions := make([]primitive.Partition, len(groupProto.Partitions))
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
		session, err := primitive.NewSession(ctx, partition, primitive.WithSessionTimeout(c.sessionTimeout))
		if err != nil {
			return nil, err
		}
		sessions[i] = session
	}

	return &PartitionGroup{
		Namespace:     groupProto.ID.Namespace,
		Name:          groupProto.ID.Name,
		Partitions:    int(groupProto.Spec.Partitions),
		PartitionSize: int(groupProto.Spec.PartitionSize),
		scope:         c.scope,
		sessions:      sessions,
	}, nil
}

// DeleteGroup deletes a partition group via the controller
// Deprecated: Groups have been replaced with Databases and can only be modified by the controller
//nolint:staticcheck
func (c *Client) DeleteGroup(ctx context.Context, name string) error {
	client := controllerapi.NewControllerServiceClient(c.conn)
	request := &controllerapi.DeletePartitionGroupRequest{
		ID: &controllerapi.PartitionGroupId{
			Name:      name,
			Namespace: c.namespace,
		},
	}
	_, err := client.DeletePartitionGroup(ctx, request)
	return err
}

// Close closes the client
func (c *Client) Close() error {
	var result error
	for _, conn := range c.conns {
		err := conn.Close()
		if err != nil {
			result = err
		}
	}

	if err := c.conn.Close(); err != nil {
		return err
	}
	return result
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

// GetMap gets or creates a Map with the given name
func (d *Database) GetMap(ctx context.Context, name string) (_map.Map, error) {
	return _map.New(ctx, primitive.NewName(d.Namespace, d.Name, d.scope, name), d.sessions)
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
// Deprecated: PartitionGroup has been replaced by the Database abstraction
type PartitionGroup struct {
	Namespace     string
	Name          string
	Partitions    int
	PartitionSize int

	scope    string
	sessions []*primitive.Session
}

// GetCounter gets or creates a Counter with the given name
func (g *PartitionGroup) GetCounter(ctx context.Context, name string) (counter.Counter, error) {
	return counter.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}

// GetElection gets or creates an Election with the given name
func (g *PartitionGroup) GetElection(ctx context.Context, name string, opts ...election.Option) (election.Election, error) {
	return election.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}

// GetIndexedMap gets or creates a Map with the given name
func (g *PartitionGroup) GetIndexedMap(ctx context.Context, name string) (indexedmap.IndexedMap, error) {
	return indexedmap.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}

// GetLeaderLatch gets or creates a LeaderLatch with the given name
func (g *PartitionGroup) GetLeaderLatch(ctx context.Context, name string, opts ...leader.Option) (leader.Latch, error) {
	return leader.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}

// GetList gets or creates a List with the given name
func (g *PartitionGroup) GetList(ctx context.Context, name string) (list.List, error) {
	return list.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}

// GetLock gets or creates a Lock with the given name
func (g *PartitionGroup) GetLock(ctx context.Context, name string) (lock.Lock, error) {
	return lock.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}

// GetMap gets or creates a Map with the given name
func (g *PartitionGroup) GetMap(ctx context.Context, name string) (_map.Map, error) {
	return _map.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}

// GetSet gets or creates a Set with the given name
func (g *PartitionGroup) GetSet(ctx context.Context, name string) (set.Set, error) {
	return set.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}

// GetValue gets or creates a Value with the given name
func (g *PartitionGroup) GetValue(ctx context.Context, name string) (value.Value, error) {
	return value.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}
