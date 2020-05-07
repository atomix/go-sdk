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
	partitionapi "github.com/atomix/api/proto/atomix/partition"
	"github.com/atomix/go-client/pkg/client/cluster"
	"sync"
)

// ID is a partition identifier
type ID int

// Partition manages a partition
type Partition struct {
	ID         ID
	Namespace  string
	Name       string
	member     *Member
	membership *Membership
	lastUpdate *partitionapi.Partition
	watchers   []chan<- Membership
	mu         sync.RWMutex
}

// Member returns the local member
func (p *Partition) Member() *Member {
	return p.member
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
	p.mu.Lock()

	oldMembers := make(map[MemberID]*Member)
	if p.membership != nil {
		for _, member := range p.membership.Members {
			oldMembers[member.ID] = member
		}
	}

	newMembers := make([]*Member, 0, len(group.Members))
	for _, member := range group.Members {
		memberID := MemberID(member.ID.Name)
		oldMember, ok := oldMembers[memberID]
		if ok {
			newMembers = append(newMembers, oldMember)
		} else {
			newMembers = append(newMembers, &Member{
				ID: memberID,
				Member: &cluster.Member{
					ID:   cluster.MemberID(memberID),
					Host: member.Host,
					Port: int(member.Port),
				},
			})
		}
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
		Members:    newMembers,
	}

	p.membership = &membership
	p.mu.Unlock()

	p.mu.RLock()
	for _, watcher := range p.watchers {
		watcher <- membership
	}
	p.mu.RUnlock()
}
