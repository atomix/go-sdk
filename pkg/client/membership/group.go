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
	controllerapi "github.com/atomix/api/proto/atomix/controller"
	"github.com/atomix/go-client/pkg/client/cluster"
	"google.golang.org/grpc"
	"io"
	"sync"
	"time"
)

// NewGroup creates a new MembershipGroup
func NewGroup(name string, address string, opts ...GroupOption) (*Group, error) {
	options := applyOptions(opts...)

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	group := &Group{
		Namespace: options.namespace,
		Name:      name,
		conn:     conn,
		options:  options,
		watchers: make([]chan<- Membership, 0),
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

// Group manages the primitives in a membership group
type Group struct {
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
				members := make([]cluster.Member, 0, len(response.Group.Members))
				for _, member := range response.Group.Members {
					members = append(members, cluster.Member{
						ID: cluster.MemberID(member.ID.Name),
					})
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
	Leadership *Leadership
	Members    []cluster.Member
}

// TermID is a leadership term
type TermID uint64

// Leadership is a group leadership
type Leadership struct {
	Leader cluster.MemberID
	Term   TermID
}
