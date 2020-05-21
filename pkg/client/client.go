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
	primitiveapi "github.com/atomix/api/proto/atomix/primitive"
	"sort"
	"time"

	databaseapi "github.com/atomix/api/proto/atomix/database"
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
	"github.com/atomix/go-client/pkg/client/util/net"
	"github.com/atomix/go-client/pkg/client/value"
	"google.golang.org/grpc"
)

// New returns a new Atomix client
func New(address string, opts ...Option) (*Client, error) {
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
	}, nil
}

// NewClient returns a new Atomix client
// Deprected: use New instead
func NewClient(address string, opts ...Option) (*Client, error) {
	return New(address, opts...)
}

// Client is an Atomix client
type Client struct {
	scope          string
	namespace      string
	sessionTimeout time.Duration
	conn           *grpc.ClientConn
}

// GetDatabases returns a list of all databases in the client's namespace
func (c *Client) GetDatabases(ctx context.Context) ([]*Database, error) {
	client := databaseapi.NewDatabaseServiceClient(c.conn)
	request := &databaseapi.GetDatabasesRequest{
		Namespace: c.namespace,
	}

	response, err := client.GetDatabases(ctx, request)
	if err != nil {
		return nil, err
	}

	databases := make([]*Database, len(response.Databases))
	for i, databaseProto := range response.Databases {
		database, err := c.newDatabase(ctx, &databaseProto)
		if err != nil {
			return nil, err
		}
		databases[i] = database
	}
	return databases, nil
}

// GetDatabase gets a database client by name from the client's namespace
func (c *Client) GetDatabase(ctx context.Context, name string) (*Database, error) {
	client := databaseapi.NewDatabaseServiceClient(c.conn)
	request := &databaseapi.GetDatabaseRequest{
		ID: databaseapi.DatabaseId{
			Name:      name,
			Namespace: c.namespace,
		},
	}

	response, err := client.GetDatabase(ctx, request)
	if err != nil {
		return nil, err
	} else if response.Database == nil {
		return nil, errors.New("unknown database " + name)
	}
	return c.newDatabase(ctx, response.Database)
}

func (c *Client) newDatabase(ctx context.Context, databaseProto *databaseapi.Database) (*Database, error) {
	// Ensure the partitions are sorted in case the controller sent them out of order.
	partitionProtos := databaseProto.Partitions
	sort.Slice(partitionProtos, func(i, j int) bool {
		return partitionProtos[i].PartitionID.Partition < partitionProtos[j].PartitionID.Partition
	})

	// Iterate through the partitions and create gRPC client connections for each partition.
	partitions := make([]primitive.Partition, len(databaseProto.Partitions))
	for i, partitionProto := range partitionProtos {
		ep := partitionProto.Endpoints[0]
		partitions[i] = primitive.Partition{
			ID:      int(partitionProto.PartitionID.Partition),
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
		conn:      c.conn,
	}, nil
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
	conn     *grpc.ClientConn
	sessions []*primitive.Session
}

// GetPrimitives gets a list of primitives in the database
func (d *Database) GetPrimitives(ctx context.Context, opts ...primitive.MetadataOption) ([]primitive.Metadata, error) {
	client := primitiveapi.NewPrimitiveServiceClient(d.conn)

	request := &primitiveapi.GetPrimitivesRequest{
		Database: &databaseapi.DatabaseId{
			Namespace: d.Namespace,
			Name:      d.Name,
		},
		Primitive: &primitiveapi.PrimitiveId{
			Namespace: d.scope,
		},
	}

	response, err := client.GetPrimitives(ctx, request)
	if err != nil {
		return nil, err
	}

	primitives := make([]primitive.Metadata, len(response.Primitives))
	for i, p := range response.Primitives {
		var primitiveType primitive.Type
		switch p.Type {
		case primitiveapi.PrimitiveType_COUNTER:
			primitiveType = "Counter"
		case primitiveapi.PrimitiveType_ELECTION:
			primitiveType = "Election"
		case primitiveapi.PrimitiveType_INDEXED_MAP:
			primitiveType = "IndexedMap"
		case primitiveapi.PrimitiveType_LEADER_LATCH:
			primitiveType = "LeaderLatch"
		case primitiveapi.PrimitiveType_LIST:
			primitiveType = "List"
		case primitiveapi.PrimitiveType_LOCK:
			primitiveType = "Lock"
		case primitiveapi.PrimitiveType_LOG:
			primitiveType = "Log"
		case primitiveapi.PrimitiveType_MAP:
			primitiveType = "Map"
		case primitiveapi.PrimitiveType_SET:
			primitiveType = "Set"
		case primitiveapi.PrimitiveType_VALUE:
			primitiveType = "Value"
		default:
			primitiveType = "Unknown"
		}
		primitives[i] = primitive.Metadata{
			Type: primitiveType,
			Name: primitive.Name{
				Scope: p.Primitive.Namespace,
				Name:  p.Primitive.Name,
			},
		}
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

// GetLog gets or creates a Log with the given name
func (g *PartitionGroup) GetLog(ctx context.Context, name string) (log.Log, error) {
	return log.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}

// GetMap gets or creates a Map with the given name
func (g *PartitionGroup) GetMap(ctx context.Context, name string, opts ..._map.Option) (_map.Map, error) {
	return _map.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions, opts...)
}

// GetSet gets or creates a Set with the given name
func (g *PartitionGroup) GetSet(ctx context.Context, name string) (set.Set, error) {
	return set.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}

// GetValue gets or creates a Value with the given name
func (g *PartitionGroup) GetValue(ctx context.Context, name string) (value.Value, error) {
	return value.New(ctx, primitive.NewName(g.Namespace, g.Name, g.scope, name), g.sessions)
}
