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

package partition

import (
	"context"
	"errors"
	"fmt"
	partitionapi "github.com/atomix/api/proto/atomix/partition"
	"github.com/atomix/go-client/pkg/client/cluster"
	"github.com/atomix/go-client/pkg/client/pb/map"
	"github.com/atomix/go-client/pkg/client/pb/replica"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-client/pkg/client/protocol"
	"google.golang.org/grpc"
	"io"
	"sync"
)

// NewGroup creates a new Group
func NewGroup(ctx context.Context, conn *grpc.ClientConn, cluster *cluster.Cluster, client *protocol.Client, opts ...Option) (*Group, error) {
	options := applyOptions(opts...)

	group := &Group{
		Client:  client,
		cluster: cluster,
		conn:    conn,
		options: options,
	}

	err := group.join(ctx)
	if err != nil {
		return nil, err
	}
	return group, nil
}

// Group manages the primitives in a partition group
type Group struct {
	*protocol.Client
	cluster      *cluster.Cluster
	conn         *grpc.ClientConn
	options      options
	partitions   []*Partition
	partitionMap map[string]*Partition
	closer       context.CancelFunc
	leaveCh      chan struct{}
	mu           sync.RWMutex
}

// Member returns the local group member
func (g *Group) Member() *Member {
	member := g.cluster.Member()
	if member == nil {
		return nil
	}
	return &Member{
		ID:     MemberID(member.ID),
		Member: member,
	}
}

// GetMap gets or creates a Map with the given name
func (g *Group) GetMap(ctx context.Context, name string, opts ..._map.Option) (_map.Map, error) {
	if g.cluster.Member() == nil {
		return nil, fmt.Errorf("cannot create peer-to-peer map: not a member of group %s", g.Name)
	}
	partitions := make([]*replica.Group, 0)
	for _, partition := range g.partitions {
		group, err := replica.NewGroup(replica.GroupID(partition.ID), g.cluster.Member(), &partitionReplicaProvider{partition})
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, group)
	}
	return _map.New(ctx, primitive.NewName(g.Namespace, g.Name, g.Scope, name), partitions, opts...)
}

func (g *Group) Partitions() []*Partition {
	return g.partitions
}

func (g *Group) Partition(id ID) *Partition {
	return g.partitionMap[fmt.Sprintf("%s-%d", g.Name, id)]
}

// join joins the partition group
func (g *Group) join(ctx context.Context) error {
	var memberID *partitionapi.MemberId
	if g.cluster.Member() != nil {
		memberID = &partitionapi.MemberId{
			Namespace: g.cluster.Namespace,
			Name:      string(g.cluster.Member().ID),
		}
	}

	g.partitions = make([]*Partition, 0, g.options.partitions)
	g.partitionMap = make(map[string]*Partition)
	for i := 1; i <= g.options.partitions; i++ {
		partition := &Partition{
			ID:        ID(i),
			Namespace: g.Namespace,
			Name:      fmt.Sprintf("%s-%d", g.Name, i),
			member:    g.Member(),
			watchers:  make([]chan<- Membership, 0),
		}
		g.partitions = append(g.partitions, partition)
		g.partitionMap[partition.Name] = partition
	}

	client := partitionapi.NewPartitionServiceClient(g.conn)
	request := &partitionapi.JoinPartitionGroupRequest{
		MemberID: memberID,
		GroupID: partitionapi.PartitionGroupId{
			Namespace: g.Namespace,
			Name:      g.Name,
		},
		Partitions:        uint32(g.options.partitions),
		ReplicationFactor: uint32(g.options.replicationFactor),
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	stream, err := client.JoinPartitionGroup(streamCtx, request)
	if err != nil {
		return err
	}

	g.mu.Lock()
	joinCh := make(chan struct{})
	g.closer = cancel
	leaveCh := make(chan struct{})
	g.leaveCh = leaveCh
	g.mu.Unlock()

	go func() {
		joined := false
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				close(leaveCh)
				return
			} else if err != nil {
				fmt.Println(err)
				close(leaveCh)
				return
			} else {
				for _, partition := range response.Group.Partitions {
					group, ok := g.partitionMap[partition.ID.Name]
					if !ok {
						continue
					}
					group.update(partition)
				}
				if !joined {
					close(joinCh)
					joined = true
				}
			}
		}
	}()

	select {
	case <-joinCh:
		return nil
	case <-ctx.Done():
		return errors.New("join timed out")
	}
}

// Close closes the partition group
func (g *Group) Close(ctx context.Context) error {
	g.mu.RLock()
	closer := g.closer
	leaveCh := g.leaveCh
	g.mu.RUnlock()
	if closer != nil {
		closer()
		select {
		case <-leaveCh:
			return nil
		case <-ctx.Done():
			return errors.New("leave timed out")
		}
	}
	return nil
}

var _ _map.Client = &Group{}

// MemberID is a partition group member identifier
type MemberID string

// Member is a partition group member
type Member struct {
	ID MemberID
	*cluster.Member
}

// Membership is a set of members
type Membership struct {
	Leadership *Leadership
	Members    []*Member
}

// TermID is a leadership term
type TermID uint64

// Leadership is a group leadership
type Leadership struct {
	Leader MemberID
	Term   TermID
}

// partitionReplicaProvider is a Provider for a Group
type partitionReplicaProvider struct {
	partition *Partition
}

func (p *partitionReplicaProvider) Watch(ctx context.Context, ch chan<- replica.Set) error {
	membershipCh := make(chan Membership)
	err := p.partition.Watch(ctx, membershipCh)
	if err != nil {
		return err
	}
	go func() {
		for membership := range membershipCh {
			var term replica.Term
			var primary *replica.Replica
			backups := make([]replica.Replica, 0)
			if membership.Leadership != nil {
				term = replica.Term(membership.Leadership.Term)
				for _, member := range membership.Members {
					if member.ID == membership.Leadership.Leader {
						primary = &replica.Replica{
							ID:     replica.ID(member.ID),
							Member: member.Member,
						}
					} else {
						backups = append(backups, replica.Replica{
							ID:     replica.ID(member.ID),
							Member: member.Member,
						})
					}
				}
			}
			ch <- replica.Set{
				Term:    term,
				Primary: primary,
				Backups: backups,
			}
		}
	}()
	return nil
}

var _ replica.Provider = &partitionReplicaProvider{}
