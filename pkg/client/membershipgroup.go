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

// MembershipGroup manages the primitives in a membership group
type MembershipGroup struct {
	Namespace  string
	Name       string
	client     *Client
	membership *Membership
	watchers   []chan<- Membership
	closer     context.CancelFunc
	leaveCh    chan struct{}
	mu         sync.RWMutex
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
	if g.client.options.memberID != "" {
		memberID = &controllerapi.MemberId{
			Namespace: g.client.options.namespace,
			Name:      g.client.options.memberID,
		}
	}

	client := controllerapi.NewMembershipGroupServiceClient(g.client.conn)
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
				members := make([]Member, 0, len(response.Group.Members))
				for _, member := range response.Group.Members {
					members = append(members, Member{
						ID: MemberID(member.ID.Name),
					})
				}
				var leadership *Leadership
				if response.Group.Leader != nil {
					leadership = &Leadership{
						Leader: MemberID(response.Group.Leader.Name),
						Term:   TermID(response.Group.Term),
					}
				}
				membership := Membership{
					Leadership: leadership,
					Members:    members,
				}

				g.mu.Lock()
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

// MemberID is a member identifier
type MemberID string

// Member is a membership group member
type Member struct {
	ID MemberID
}

// Membership is a set of members
type Membership struct {
	Leadership *Leadership
	Members    []Member
}

// TermID is a leadership term
type TermID uint64

// Leadership is a group leadership
type Leadership struct {
	Leader MemberID
	Term   TermID
}
