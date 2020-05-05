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
	"github.com/atomix/go-client/pkg/client/protocol"
	"io"
	"sync"
	"time"
)

// NewGroup creates a new Group
func NewGroup(ctx context.Context, protocol *protocol.Protocol, opts ...Option) (*Group, error) {
	options := applyOptions(opts...)

	group := &Group{
		Protocol: protocol,
		options:  options,
	}

	err := group.join(context.Background())
	if err != nil {
		return nil, err
	}
	return group, nil
}

// Group manages the primitives in a partition group
type Group struct {
	*protocol.Protocol
	options      options
	partitions   []*Partition
	partitionMap map[string]*Partition
	closer       context.CancelFunc
	leaveCh      chan struct{}
	mu           sync.RWMutex
}

func (g *Group) Partitions() []*Partition {
	return g.partitions
}

func (g *Group) Partition(id PartitionID) *Partition {
	return g.partitionMap[fmt.Sprintf("%s-%d", g.Name, id)]
}

// join joins the partition group
func (g *Group) join(ctx context.Context) error {
	var memberID *partitionapi.MemberId
	if g.options.memberID != "" {
		memberID = &partitionapi.MemberId{
			Namespace: g.options.namespace,
			Name:      g.options.memberID,
		}
	}

	g.partitions = make([]*Partition, 0, g.options.partitions)
	g.partitionMap = make(map[string]*Partition)
	for i := 1; i <= g.options.partitions; i++ {
		partition := &Partition{
			ID:        PartitionID(i),
			Namespace: g.Namespace,
			Name:      fmt.Sprintf("%s-%d", g.Name, i),
			watchers:  make([]chan<- Membership, 0),
		}
		g.partitions = append(g.partitions, partition)
		g.partitionMap[partition.partition.Name] = partition
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
func (g *Group) Close() error {
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
	Namespace  string
	Name       string
	partition  *Partition
	membership *Membership
	lastUpdate *partitionapi.Partition
	watchers   []chan<- Membership
	mu         sync.RWMutex
}

// Membership returns the current group membership
func (p *Partition) Membership() Membership {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.membership != nil {
		return *p.membership
	}
	return Membership{}
}

// Watch watches the membership for changes
func (p *Partition) Watch(ctx context.Context, ch chan<- Membership) error {
	p.mu.Lock()
	watcher := make(chan Membership)
	membership := p.membership
	go func() {
		if membership != nil {
			ch <- *membership
		}
		for {
			select {
			case membership, ok := <-watcher:
				if !ok {
					return
				}
				ch <- membership
			case <-ctx.Done():
				p.mu.Lock()
				watchers := make([]chan<- Membership, 0)
				for _, ch := range p.watchers {
					if ch != watcher {
						watchers = append(watchers, ch)
					}
				}
				p.watchers = watchers
				p.mu.Unlock()
				close(watcher)
			}
		}
	}()
	p.watchers = append(p.watchers, watcher)
	p.mu.Unlock()
	return nil
}

func (p *Partition) update(group partitionapi.Partition) {
	if p.lastUpdate != nil && (*p.lastUpdate).String() == group.String() {
		return
	}
	p.partition.mu.Lock()

	oldMembers := make(map[cluster.MemberID]*cluster.Member)
	if p.partition.membership != nil {
		for _, member := range p.partition.membership.Members {
			oldMembers[member.ID] = member
		}
	}

	newMembers := make([]*cluster.Member, 0, len(group.Members))
	for _, member := range group.Members {
		memberID := cluster.MemberID(member.ID.Name)
		oldMember, ok := oldMembers[memberID]
		if ok {
			newMembers = append(newMembers, oldMember)
		} else {
			newMembers = append(newMembers, &cluster.Member{
				ID:   memberID,
				Host: member.Host,
				Port: int(member.Port),
			})
		}
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
		Members:    newMembers,
	}

	p.partition.mu.Lock()
	p.partition.membership = &membership
	p.partition.mu.Unlock()

	p.partition.mu.RLock()
	for _, watcher := range p.partition.watchers {
		watcher <- membership
	}
	p.partition.mu.RUnlock()
}

// Membership is a set of members
type Membership struct {
	Leadership *Leadership
	Members    []*cluster.Member
}

// TermID is a leadership term
type TermID uint64

// Leadership is a group leadership
type Leadership struct {
	Leader cluster.MemberID
	Term   TermID
}
