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

package group

import (
	"context"
	"errors"
	"fmt"
	controllerapi "github.com/atomix/api/proto/atomix/controller"
	"github.com/atomix/go-client/pkg/client/cluster"
	"google.golang.org/grpc"
	"io"
	"sync"
	"time"
)

// NewPartitionGroup creates a new PartitionGroup
func NewPartitionGroup(name string, address string, opts ...PartitionGroupOption) (*PartitionGroup, error) {
	options := applyPartitionGroupOptions(opts...)

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	group := &PartitionGroup{
		Namespace: options.namespace,
		Name:      name,
		conn:      conn,
		options:   options,
	}

	if options.joinTimeout != nil {
		ctx, cancel := context.WithTimeout(context.Background(), *options.joinTimeout)
		err = group.join(ctx)
		cancel()
	} else {
		err = group.join(context.Background())
	}
	if err != nil {
		return nil, err
	}
	return group, nil
}

// PartitionGroup manages the primitives in a partition group
type PartitionGroup struct {
	conn         *grpc.ClientConn
	Namespace    string
	Name         string
	options      partitionGroupOptions
	partitions   []*Partition
	partitionMap map[string]*Partition
	closer       context.CancelFunc
	leaveCh      chan struct{}
	mu           sync.RWMutex
}

func (g *PartitionGroup) Partitions() []*Partition {
	return g.partitions
}

func (g *PartitionGroup) Partition(id PartitionID) *Partition {
	return g.partitionMap[fmt.Sprintf("%s-%d", g.Name, id)]
}

// join joins the partition group
func (g *PartitionGroup) join(ctx context.Context) error {
	var memberID *controllerapi.MemberId
	if g.options.memberID != "" {
		memberID = &controllerapi.MemberId{
			Namespace: g.options.namespace,
			Name:      g.options.memberID,
		}
	}

	g.partitions = make([]*Partition, 0, g.options.partitions)
	g.partitionMap = make(map[string]*Partition)
	for i := 1; i <= g.options.partitions; i++ {
		groupOptions := groupOptions{
			memberID:  g.options.memberID,
			scope:     g.options.scope,
			namespace: g.options.namespace,
		}
		partition := &Partition{
			ID: PartitionID(i),
			group: &MembershipGroup{
				Namespace: g.Namespace,
				Name:      fmt.Sprintf("%s-%d", g.Name, i),
				options:   groupOptions,
				watchers:  make([]chan<- Membership, 0),
			},
		}
		g.partitions = append(g.partitions, partition)
		g.partitionMap[partition.group.Name] = partition
	}

	client := controllerapi.NewPartitionGroupServiceClient(g.conn)
	request := &controllerapi.JoinPartitionGroupRequest{
		MemberID: memberID,
		GroupID: controllerapi.PartitionGroupId{
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
func (g *PartitionGroup) Close() error {
	g.mu.RLock()
	closer := g.closer
	leaveCh := g.leaveCh
	g.mu.RUnlock()
	if closer != nil {
		closer()
		timeout := 30 * time.Second
		if g.options.joinTimeout != nil {
			timeout = *g.options.joinTimeout
		}
		select {
		case <-leaveCh:
			return nil
		case <-time.After(timeout):
			return errors.New("leave timed out")
		}
	}
	return nil
}

// PartitionID is a partition identifier
type PartitionID int

// Partition manages a partition
type Partition struct {
	ID         PartitionID
	group      *MembershipGroup
	lastUpdate *controllerapi.MembershipGroup
}

func (p *Partition) update(group controllerapi.MembershipGroup) {
	if p.lastUpdate != nil && (*p.lastUpdate).String() == group.String() {
		return
	}

	members := make([]cluster.Member, 0, len(group.Members))
	for _, member := range group.Members {
		members = append(members, cluster.Member{
			ID: cluster.MemberID(member.ID.Name),
		})
	}
	var leadership *Leadership
	if group.Leader != nil {
		leadership = &Leadership{
			Leader: cluster.MemberID(group.Leader.Name),
			Term:   TermID(group.Term),
		}
	}
	membership := Membership{
		Leadership: leadership,
		Members:    members,
	}

	p.group.mu.Lock()
	p.group.membership = &membership
	p.group.mu.Unlock()

	p.group.mu.RLock()
	for _, watcher := range p.group.watchers {
		watcher <- membership
	}
	p.group.mu.RUnlock()
}

func (p *Partition) MembershipGroup() *MembershipGroup {
	return p.group
}
