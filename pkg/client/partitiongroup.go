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
	"io"
	"sync"
	"time"
)

// PartitionGroup manages the primitives in a partition group
type PartitionGroup struct {
	Namespace    string
	Name         string
	client       *Client
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
	if g.client.options.memberID == "" {
		return nil
	}

	g.partitions = make([]*Partition, 0, g.options.partitions)
	g.partitionMap = make(map[string]*Partition)
	for i := 1; i <= g.options.partitions; i++ {
		partition := &Partition{
			ID: PartitionID(i),
			group: &MembershipGroup{
				Namespace: g.Namespace,
				Name:      fmt.Sprintf("%s-%d", g.Name, i),
				client:    g.client,
				watchers:  make([]chan<- Membership, 0),
			},
		}
		g.partitions = append(g.partitions, partition)
		g.partitionMap[partition.group.Name] = partition
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
		if g.client.options.joinTimeout != nil {
			timeout = *g.client.options.joinTimeout
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

	members := make([]Member, 0, len(group.Members))
	for _, member := range group.Members {
		members = append(members, Member{
			ID: MemberID(member.ID.Name),
		})
	}
	var leadership *Leadership
	if group.Leader != nil {
		leadership = &Leadership{
			Leader: MemberID(group.Leader.Name),
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
