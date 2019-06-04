package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-client/pkg/client/_map"
	"github.com/atomix/atomix-go-client/pkg/client/counter"
	"github.com/atomix/atomix-go-client/pkg/client/election"
	"github.com/atomix/atomix-go-client/pkg/client/lock"
	"github.com/atomix/atomix-go-client/pkg/client/protocol"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/set"
	"github.com/atomix/atomix-go-client/proto/atomix/controller"
	"github.com/atomix/atomix-go-client/proto/atomix/partition"
	"google.golang.org/grpc"
	"net"
)

// NewClient returns a new Atomix client
func NewClient(address string, opts ...ClientOption) (*Client, error) {
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

// Atomix primitive client
type Client struct {
	application string
	namespace   string
	conn        *grpc.ClientConn
	conns       []*grpc.ClientConn
}

// CreateGroup creates a new partition group
func (c *Client) CreateGroup(ctx context.Context, name string, partitions int, partitionSize int, protocol protocol.Protocol) (*PartitionGroup, error) {
	client := controller.NewControllerServiceClient(c.conn)
	request := &controller.CreatePartitionGroupRequest{
		Id: &partition.PartitionGroupId{
			Name:      name,
			Namespace: c.namespace,
		},
		Spec: &partition.PartitionGroupSpec{
			Partitions:    uint32(partitions),
			PartitionSize: uint32(partitionSize),
			Group:         protocol.Spec().GetGroup(),
		},
	}

	_, err := client.CreatePartitionGroup(ctx, request)
	if err != nil {
		return nil, err
	}
	return c.GetGroup(ctx, name)
}

// GetGroup returns a partition group primitive client
func (c *Client) GetGroup(ctx context.Context, name string) (*PartitionGroup, error) {
	client := controller.NewControllerServiceClient(c.conn)
	request := &controller.GetPartitionGroupsRequest{
		Id: &partition.PartitionGroupId{
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

	group := response.Groups[0]
	partitions := make([]*grpc.ClientConn, len(group.Partitions))
	for i, partition := range group.Partitions {
		ep := partition.Endpoints[0]
		conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ep.Host, ep.Port), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		partitions[i] = conn
		c.conns = append(c.conns, conn)
	}
	return &PartitionGroup{
		Namespace:   c.namespace,
		Name:        name,
		application: c.application,
		partitions:  partitions,
	}, nil
}

// DeleteGroup deletes a partition group via the controller
func (c *Client) DeleteGroup(ctx context.Context, name string) error {
	client := controller.NewControllerServiceClient(c.conn)
	request := &controller.DeletePartitionGroupRequest{
		Id: &partition.PartitionGroupId{
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

// NewGroup returns a partition group client
func NewGroup(address string, opts ...ClientOption) (*PartitionGroup, error) {
	_, records, err := net.LookupSRV("", "", address)
	if err != nil {
		return nil, err
	}

	options := applyOptions(opts...)
	partitions := make([]*grpc.ClientConn, len(records))
	for i, record := range records {
		conn, err := grpc.Dial(fmt.Sprintf("%s:%d", record.Target, record.Port), grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		partitions[i] = conn
	}
	return &PartitionGroup{
		Namespace:   options.namespace,
		Name:        address,
		application: options.application,
		partitions:  partitions,
	}, nil
}

// Primitive partition group.
type PartitionGroup struct {
	counter.CounterClient
	_map.MapClient
	election.ElectionClient
	lock.LockClient
	set.SetClient

	Namespace string
	Name      string

	application string
	partitions  []*grpc.ClientConn
}

func (g *PartitionGroup) GetCounter(ctx context.Context, name string, opts ...session.SessionOption) (counter.Counter, error) {
	return counter.New(ctx, g.application, name, g.partitions, opts...)
}

func (g *PartitionGroup) GetElection(ctx context.Context, name string, opts ...session.SessionOption) (election.Election, error) {
	return election.New(ctx, g.application, name, g.partitions, opts...)
}

func (g *PartitionGroup) GetLock(ctx context.Context, name string, opts ...session.SessionOption) (lock.Lock, error) {
	return lock.New(ctx, g.application, name, g.partitions, opts...)
}

func (g *PartitionGroup) GetMap(ctx context.Context, name string, opts ...session.SessionOption) (_map.Map, error) {
	return _map.New(ctx, g.application, name, g.partitions, opts...)
}

func (g *PartitionGroup) GetSet(ctx context.Context, name string, opts ...session.SessionOption) (set.Set, error) {
	return set.New(ctx, g.application, name, g.partitions, opts...)
}
