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
	controllerapi "github.com/atomix/atomix-api/proto/atomix/controller"
	primitiveapi "github.com/atomix/atomix-api/proto/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/counter"
	"github.com/atomix/atomix-go-client/pkg/client/election"
	"github.com/atomix/atomix-go-client/pkg/client/list"
	"github.com/atomix/atomix-go-client/pkg/client/lock"
	"github.com/atomix/atomix-go-client/pkg/client/map"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/set"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"github.com/atomix/atomix-go-client/pkg/client/value"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
		conn:        conn,
		application: options.application,
		namespace:   options.namespace,
		conns:       []*grpc.ClientConn{},
	}, nil
}

// Client is an Atomix client
type Client struct {
	application string
	namespace   string
	conn        *grpc.ClientConn
	conns       []*grpc.ClientConn
}

// CreateGroup creates a new partition group
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
		group, err := c.newGroup(groupProto)
		if err != nil {
			return nil, err
		}
		groups[i] = group
	}
	return groups, nil
}

// GetGroup returns a partition group primitive client
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
	return c.newGroup(response.Groups[0])
}

func (c *Client) newGroup(groupProto *controllerapi.PartitionGroup) (*PartitionGroup, error) {
	// Ensure the partitions are sorted in case the controller sent them out of order.
	partitionProtos := groupProto.Partitions
	sort.Slice(partitionProtos, func(i, j int) bool {
		return partitionProtos[i].PartitionID < partitionProtos[j].PartitionID
	})

	// Iterate through the partitions and create gRPC client connections for each partition.
	partitions := make([]*grpc.ClientConn, len(groupProto.Partitions))
	for i, partitionProto := range partitionProtos {
		ep := partitionProto.Endpoints[0]
		conn, err := grpc.Dial(
			fmt.Sprintf("%s:%d", ep.Host, ep.Port),
			grpc.WithInsecure(),
			grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(
				grpc_retry.WithMax(100),
				grpc_retry.WithBackoff(grpc_retry.BackoffExponential(10*time.Millisecond)),
				grpc_retry.WithPerRetryTimeout(5*time.Second),
				grpc_retry.WithCodes(codes.Unavailable))),
			grpc.WithStreamInterceptor(grpc_retry.StreamClientInterceptor(
				grpc_retry.WithMax(100),
				grpc_retry.WithBackoff(grpc_retry.BackoffExponential(10*time.Millisecond)),
				grpc_retry.WithPerRetryTimeout(5*time.Second),
				grpc_retry.WithCodes(codes.Unavailable))))
		if err != nil {
			return nil, err
		}
		partitions[i] = conn
		c.conns = append(c.conns, conn)
	}

	return &PartitionGroup{
		Namespace:     groupProto.ID.Namespace,
		Name:          groupProto.ID.Name,
		Partitions:    int(groupProto.Spec.Partitions),
		PartitionSize: int(groupProto.Spec.PartitionSize),
		Protocol:      groupProto.Spec.Protocol.TypeUrl,
		application:   c.application,
		partitions:    partitions,
	}, nil
}

// DeleteGroup deletes a partition group via the controller
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

// PartitionGroup manages the primitives in a partition group
type PartitionGroup struct {
	Namespace     string
	Name          string
	Partitions    int
	PartitionSize int
	Protocol      string

	application string
	partitions  []*grpc.ClientConn
}

// GetPrimitives gets a list of primitives of the given types
func (g *PartitionGroup) GetPrimitives(ctx context.Context, types ...primitive.Type) ([]*primitiveapi.PrimitiveInfo, error) {
	if len(types) == 0 {
		return g.getPrimitives(ctx, "")
	}

	primitives := []*primitiveapi.PrimitiveInfo{}
	for _, t := range types {
		typePrimitives, err := g.getPrimitives(ctx, t)
		if err != nil {
			return nil, err
		}
		primitives = append(primitives, typePrimitives...)
	}
	return primitives, nil
}

// getPrimitives gets a list of primitives of the given type
func (g *PartitionGroup) getPrimitives(ctx context.Context, t primitive.Type) ([]*primitiveapi.PrimitiveInfo, error) {
	results, err := util.ExecuteAsync(len(g.partitions), func(i int) (i2 interface{}, e error) {
		client := primitiveapi.NewPrimitiveServiceClient(g.partitions[i])
		request := &primitiveapi.GetPrimitivesRequest{
			Type:      string(t),
			Namespace: g.application,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		response, err := client.GetPrimitives(ctx, request)
		if err != nil {
			return nil, err
		}
		return response.Primitives, nil
	})

	if err != nil {
		return nil, err
	}

	primitiveResults := make(map[string]*primitiveapi.PrimitiveInfo)
	for _, result := range results {
		primitives := result.([]*primitiveapi.PrimitiveInfo)
		for _, info := range primitives {
			primitiveResults[info.Name.String()] = info
		}
	}

	primitives := make([]*primitiveapi.PrimitiveInfo, 0, len(primitiveResults))
	for _, info := range primitiveResults {
		primitives = append(primitives, info)
	}
	return primitives, nil
}

// GetCounter gets or creates a Counter with the given name
func (g *PartitionGroup) GetCounter(ctx context.Context, name string, opts ...session.Option) (counter.Counter, error) {
	return counter.New(ctx, primitive.NewName(g.Namespace, g.Name, g.application, name), g.partitions, opts...)
}

// GetElection gets or creates an Election with the given name
func (g *PartitionGroup) GetElection(ctx context.Context, name string, opts ...session.Option) (election.Election, error) {
	return election.New(ctx, primitive.NewName(g.Namespace, g.Name, g.application, name), g.partitions, opts...)
}

// GetList gets or creates a List with the given name
func (g *PartitionGroup) GetList(ctx context.Context, name string, opts ...session.Option) (list.List, error) {
	return list.New(ctx, primitive.NewName(g.Namespace, g.Name, g.application, name), g.partitions, opts...)
}

// GetLock gets or creates a Lock with the given name
func (g *PartitionGroup) GetLock(ctx context.Context, name string, opts ...session.Option) (lock.Lock, error) {
	return lock.New(ctx, primitive.NewName(g.Namespace, g.Name, g.application, name), g.partitions, opts...)
}

// GetMap gets or creates a Map with the given name
func (g *PartitionGroup) GetMap(ctx context.Context, name string, opts ...session.Option) (_map.Map, error) {
	return _map.New(ctx, primitive.NewName(g.Namespace, g.Name, g.application, name), g.partitions, opts...)
}

// GetSet gets or creates a Set with the given name
func (g *PartitionGroup) GetSet(ctx context.Context, name string, opts ...session.Option) (set.Set, error) {
	return set.New(ctx, primitive.NewName(g.Namespace, g.Name, g.application, name), g.partitions, opts...)
}

// GetValue gets or creates a Value with the given name
func (g *PartitionGroup) GetValue(ctx context.Context, name string, opts ...session.Option) (value.Value, error) {
	return value.New(ctx, primitive.NewName(g.Namespace, g.Name, g.application, name), g.partitions, opts...)
}
