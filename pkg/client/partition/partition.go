package partition

import (
	"github.com/atomix/atomix-go-client/pkg/client/_map"
	"github.com/atomix/atomix-go-client/pkg/client/election"
	"github.com/atomix/atomix-go-client/pkg/client/lock"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/set"
	"google.golang.org/grpc"
	"sort"
)

func NewPartitionGroup(application string, namespace string, name string, partitions []*Partition) (*PartitionGroup, error) {
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Id < partitions[j].Id
	})
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
	partitions  []*Partition
}

// getConnections returns a sorted slice of gRPC client connections for all partitions
func (g *PartitionGroup) getConnections() []*grpc.ClientConn {
	connections := make([]*grpc.ClientConn, len(g.partitions))
	for i, partition := range g.partitions {
		connections[i] = partition.conn
	}
	return connections
}

func (g *PartitionGroup) NewMap(name string, opts ...session.Option) (*_map.Map, error) {
	return _map.NewMap(g.Application, name, g.getConnections(), opts...)
}

func (g *PartitionGroup) NewSet(name string, opts ...session.Option) (*set.Set, error) {
	return set.NewSet(g.Application, name, g.getConnections(), opts...)
}

func (g *PartitionGroup) NewLock(name string, opts ...session.Option) (*lock.Lock, error) {
	return lock.NewLock(g.Application, name, g.getConnections(), opts...)
}

func (g *PartitionGroup) NewLeaderElection(name string, candidate string, opts ...session.Option) (*election.Election, error) {
	return election.NewElection(g.Application, name, candidate, g.getConnections(), opts...)
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

func NewPartition(id int, address string, opts ...grpc.DialOption) (*Partition, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, err
	}

	return &Partition{
		Id:   id,
		conn: conn,
	}, nil
}

// Partition is a client for a specific partition
type Partition struct {
	Id   int
	conn *grpc.ClientConn
}

// Close closes the partition
func (c *Partition) Close() error {
	return c.conn.Close()
}
