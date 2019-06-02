package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-client/pkg/client/_map"
	"github.com/atomix/atomix-go-client/pkg/client/election"
	"github.com/atomix/atomix-go-client/pkg/client/lock"
	"github.com/atomix/atomix-go-client/pkg/client/partition"
	"github.com/atomix/atomix-go-client/pkg/client/protocol"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/proto/atomix/controller"
	partitionpb "github.com/atomix/atomix-go-client/proto/atomix/partition"
	"google.golang.org/grpc"
)

// NewClient returns a new client for the controller indicated by 'address'
func NewClient(application string, namespace string, address string, opts ...grpc.DialOption) (*Client, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:        conn,
		opts:        opts,
		Application: application,
		Namespace:   namespace,
	}, nil
}

// Client is the interface to an Atomix cluster
type Client struct {
	conn        *grpc.ClientConn
	opts        []grpc.DialOption
	Application string
	Namespace   string
}

// CreatePartitionGroup creates a new partition group
func (c *Client) CreatePartitionGroup(name string, partitions int, partitionSize int, protocol protocol.Protocol) (*PartitionGroup, error) {
	client := controller.NewControllerServiceClient(c.conn)
	request := &controller.CreatePartitionGroupRequest{
		Id: &partitionpb.PartitionGroupId{
			Name:      name,
			Namespace: c.Namespace,
		},
		Spec: &partitionpb.PartitionGroupSpec{
			Partitions:    uint32(partitions),
			PartitionSize: uint32(partitionSize),
			Group:         protocol.Spec().GetGroup(),
		},
	}

	_, err := client.CreatePartitionGroup(context.Background(), request)
	if err != nil {
		return nil, err
	}
	return c.GetPartitionGroup(name)
}

// GetPartitionGroup gets a partition group in the client's namespace
func (c *Client) GetPartitionGroup(name string) (*PartitionGroup, error) {
	client := controller.NewControllerServiceClient(c.conn)
	request := &controller.GetPartitionGroupsRequest{
		Id: &partitionpb.PartitionGroupId{
			Namespace: c.Namespace,
			Name:      name,
		},
	}

	response, err := client.GetPartitionGroups(context.Background(), request)
	if err != nil {
		return nil, err
	}

	if len(response.Groups) == 0 {
		return nil, errors.New("unknown partition group " + name)
	} else if len(response.Groups) > 1 {
		return nil, errors.New("partition group " + name + " is ambiguous")
	}

	group := response.Groups[0]
	partitions := []*partition.Partition{}
	for _, partitionpb := range group.Partitions {
		ep := partitionpb.Endpoints[0]
		address := fmt.Sprintf("%s:%d", ep.Host, ep.Port)
		partition, err := partition.NewPartition(int(partitionpb.PartitionId), address)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, partition)
	}
	return newPartitionGroup(c.Application, c.Namespace, name, partitions)
}

// DeletePartitionGroup deletes a partition group via the controller
func (c *Client) DeletePartitionGroup(name string) error {
	client := controller.NewControllerServiceClient(c.conn)
	request := &controller.DeletePartitionGroupRequest{
		Id: &partitionpb.PartitionGroupId{
			Namespace: c.Namespace,
			Name:      name,
		},
	}

	_, err := client.DeletePartitionGroup(context.Background(), request)
	return err
}

// Close closes the client
func (c *Client) Close() error {
	return c.conn.Close()
}

func newPartitionGroup(application string, namespace string, name string, partitions []*partition.Partition) (*PartitionGroup, error) {
	return &PartitionGroup{
		Application: application,
		Namespace:   namespace,
		Name:        name,
		partitions:  partitions,
	}, nil
}

// PartitionGroup is the interface to an Atomix partition group
type PartitionGroup struct {
	Application string
	Name        string
	Namespace   string
	partitions  []*partition.Partition
}

func (g *PartitionGroup) NewMap(name string, protocol *protocol.Protocol, opts ...session.Option) (*_map.Map, error) {
	return _map.NewMap(g.Application, name, g.partitions, opts...)
}

func (g *PartitionGroup) NewLock(name string, protocol *protocol.Protocol, opts ...session.Option) (*lock.Lock, error) {
	return lock.NewLock(g.Application, name, g.partitions, opts...)
}

func (g *PartitionGroup) NewLeaderElection(name string, protocol *protocol.Protocol, opts ...session.Option) (*election.Election, error) {
	return election.NewElection(g.Application, name, g.partitions, opts...)
}

// Close closes the partition group clients
func (g *PartitionGroup) Close() error {
	for _, p := range g.partitions {
		if err := p.Close(); err != nil {
			return err
		}
	}
	return nil
}
