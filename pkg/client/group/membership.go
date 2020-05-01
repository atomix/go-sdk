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
	"github.com/atomix/go-client/pkg/client/p2p/map"
	"github.com/atomix/go-client/pkg/client/p2p/primitive"
	"google.golang.org/grpc"
	"io"
	"sync"
	"time"
)

// NewMembershipGroup creates a new MembershipGroup
func NewMembershipGroup(name string, address string, opts ...MembershipGroupOption) (*MembershipGroup, error) {
	options := applyOptions(opts...)

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	group := &MembershipGroup{
		Namespace: options.namespace,
		Name:      name,
		conn:      conn,
		options:   options,
		watchers:  make([]chan<- Membership, 0),
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

// MembershipGroup manages the primitives in a membership group
type MembershipGroup struct {
	conn       *grpc.ClientConn
	options    groupOptions
	Namespace  string
	Name       string
	membership *Membership
	watchers   []chan<- Membership
	closer     context.CancelFunc
	leaveCh    chan struct{}
	mu         sync.RWMutex
}

// GetMap gets or creates a Map with the given name
func (g *MembershipGroup) GetMap(ctx context.Context, name string, opts ..._map.Option) (_map.Map, error) {
	provider := &membershipGroupPeerProvider{g}
	peers, err := primitive.NewPeerGroup(provider)
	if err != nil {
		return nil, err
	}
	return _map.NewGossipMap(ctx, primitive.NewName(g.Namespace, g.Name, g.options.scope, name), peers, opts...)
}

// Membership returns the current group membership
func (g *MembershipGroup) Membership() Membership {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.membership != nil {
		return *g.membership
	}
	return Membership{}
}

// Watch watches the membership for changes
func (g *MembershipGroup) Watch(ctx context.Context, ch chan<- Membership) error {
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
func (g *MembershipGroup) join(ctx context.Context) error {
	var memberID *controllerapi.MemberId
	if g.options.memberID != "" {
		memberID = &controllerapi.MemberId{
			Namespace: g.options.namespace,
			Name:      g.options.memberID,
		}
	}

	client := controllerapi.NewMembershipGroupServiceClient(g.conn)
	request := &controllerapi.JoinMembershipGroupRequest{
		MemberID: memberID,
		GroupID: controllerapi.MembershipGroupId{
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

				var leadership *Leadership
				if response.Group.Leader != nil {
					leadership = &Leadership{
						Leader: cluster.MemberID(response.Group.Leader.Name),
						Term:   TermID(response.Group.Term),
					}
				}
				membership := Membership{
					Leadership: leadership,
					Members:    newMembers,
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
func (g *MembershipGroup) Close() error {
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

// membershipGroupPeerProvider is a PeerProvider for a MembershipGroup
type membershipGroupPeerProvider struct {
	group *MembershipGroup
}

func (p *membershipGroupPeerProvider) Watch(ctx context.Context, ch chan<- primitive.PeerSet) error {
	membershipCh := make(chan Membership)
	err := p.group.Watch(ctx, membershipCh)
	if err != nil {
		return err
	}
	go func() {
		for membership := range membershipCh {
			peers := make([]primitive.Peer, 0)
			for _, member := range membership.Members {
				if string(member.ID) != p.group.options.memberID {
					peers = append(peers, primitive.Peer{
						ID:     primitive.PeerID(member.ID),
						Member: member,
					})
				}
			}
			ch <- peers
		}
	}()
	return nil
}

var _ primitive.PeerProvider = &membershipGroupPeerProvider{}
