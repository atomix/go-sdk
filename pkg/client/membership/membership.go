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

package membership

import (
	"context"
	"errors"
	"fmt"
	membershipapi "github.com/atomix/api/proto/atomix/membership"
	"github.com/atomix/go-client/pkg/client/cluster"
	"github.com/atomix/go-client/pkg/client/gossip/map"
	"github.com/atomix/go-client/pkg/client/gossip/peer"
	"github.com/atomix/go-client/pkg/client/protocol"
	"io"
	"sync"
	"time"
)

// NewGroup creates a new Group
func NewGroup(ctx context.Context, protocol *protocol.Protocol, opts ...Option) (*Group, error) {
	group := &Group{
		Protocol: protocol,
		watchers: make([]chan<- Membership, 0),
	}

	err := group.join(ctx)
	if err != nil {
		return nil, err
	}
	return group, nil
}

// Group manages the primitives in a membership group
type Group struct {
	*protocol.Protocol
	membership *Membership
	watchers   []chan<- Membership
	closer     context.CancelFunc
	leaveCh    chan struct{}
	mu         sync.RWMutex
}

// GetMap gets or creates a Map with the given name
func (g *Group) GetMap(ctx context.Context, name string, opts ..._map.Option) (_map.Map, error) {
	provider := &membershipGroupPeerProvider{g}
	member := cluster.Member{
		ID: cluster.MemberID(g.options.memberID),
	}
	peers, err := peer.NewGroup(member, provider)
	if err != nil {
		return nil, err
	}
	return _map.NewGossipMap(ctx, peer.NewName(g.Namespace, g.Name, g.Scope, name), peers, opts...)
}

// Membership returns the current group membership
func (g *Group) Membership() Membership {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.membership != nil {
		return *g.membership
	}
	return Membership{}
}

// Watch watches the membership for changes
func (g *Group) Watch(ctx context.Context, ch chan<- Membership) error {
	g.mu.Lock()
	watcher := make(chan Membership)
	membership := g.membership
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
				g.mu.Lock()
				watchers := make([]chan<- Membership, 0)
				for _, ch := range g.watchers {
					if ch != watcher {
						watchers = append(watchers, ch)
					}
				}
				g.watchers = watchers
				g.mu.Unlock()
				close(watcher)
			}
		}
	}()
	g.watchers = append(g.watchers, watcher)
	g.mu.Unlock()
	return nil
}

// join joins the membership group
func (g *Group) join(ctx context.Context) error {
	var memberID *membershipapi.MemberId
	if g.options.memberID != "" {
		memberID = &membershipapi.MemberId{
			Namespace: g.options.namespace,
			Name:      g.options.memberID,
		}
	}

	client := membershipapi.NewMembershipServiceClient(g.conn)
	request := &membershipapi.JoinMembershipGroupRequest{
		MemberID: memberID,
		GroupID: membershipapi.MembershipGroupId{
			Namespace: g.Namespace,
			Name:      g.Name,
		},
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	stream, err := client.JoinMembershipGroup(streamCtx, request)
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
				g.mu.Lock()

				oldMembers := make(map[cluster.MemberID]*cluster.Member)
				if g.membership != nil {
					for _, member := range g.membership.Members {
						oldMembers[member.ID] = member
					}
				}

				newMembers := make([]*cluster.Member, 0, len(response.Group.Members))
				for _, member := range response.Group.Members {
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

				membership := Membership{
					Members: newMembers,
				}

				g.membership = &membership
				g.mu.Unlock()

				if !joined {
					close(joinCh)
					joined = true
				}

				g.mu.RLock()
				for _, watcher := range g.watchers {
					watcher <- membership
				}
				g.mu.RUnlock()
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

// Close closes the membership group
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

// Membership is a set of members
type Membership struct {
	Members []*cluster.Member
}

// membershipGroupPeerProvider is a Provider for a Group
type membershipGroupPeerProvider struct {
	group *Group
}

func (p *membershipGroupPeerProvider) Watch(ctx context.Context, ch chan<- peer.Set) error {
	membershipCh := make(chan Membership)
	err := p.group.Watch(ctx, membershipCh)
	if err != nil {
		return err
	}
	go func() {
		for membership := range membershipCh {
			peers := make([]peer.Peer, 0)
			for _, member := range membership.Members {
				if string(member.ID) != p.group.options.memberID {
					peers = append(peers, peer.Peer{
						ID:     peer.ID(member.ID),
						Member: member,
					})
				}
			}
			ch <- peers
		}
	}()
	return nil
}

var _ peer.Provider = &membershipGroupPeerProvider{}
