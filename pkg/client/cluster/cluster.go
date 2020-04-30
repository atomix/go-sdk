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

package cluster

import (
	"context"
	"errors"
	"fmt"
	controllerapi "github.com/atomix/api/proto/atomix/controller"
	"google.golang.org/grpc"
	"io"
	"sync"
	"time"
)

// New creates a new cluster
func New(address string, opts ...Option) (*Cluster, error) {
	options := applyOptions(opts...)

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	cluster := &Cluster{
		conn:     conn,
		options:  options,
		watchers: make([]chan<- Membership, 0),
	}

	if options.joinTimeout != nil {
		ctx, cancel := context.WithTimeout(context.Background(), *options.joinTimeout)
		err = cluster.join(ctx)
		cancel()
	} else {
		err = cluster.join(context.Background())
	}
	if err != nil {
		return nil, err
	}
	return cluster, nil
}

// Cluster manages the cluster membership for a client
type Cluster struct {
	conn       *grpc.ClientConn
	options    clusterOptions
	membership *Membership
	watchers   []chan<- Membership
	closer     context.CancelFunc
	leaveCh    chan struct{}
	mu         sync.RWMutex
}

// Membership returns the current cluster membership
func (c *Cluster) Membership() Membership {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.membership != nil {
		return *c.membership
	}
	return Membership{}
}

// join joins the cluster
func (c *Cluster) join(ctx context.Context) error {
	var member *controllerapi.Member
	if c.options.memberID != "" {
		member = &controllerapi.Member{
			ID: controllerapi.MemberId{
				Namespace: c.options.namespace,
				Name:      c.options.memberID,
			},
			Host: c.options.peerHost,
			Port: int32(c.options.peerPort),
		}
	}

	client := controllerapi.NewClusterServiceClient(c.conn)
	request := &controllerapi.JoinClusterRequest{
		Member: member,
		GroupID: controllerapi.MembershipGroupId{
			Namespace: c.options.namespace,
			Name:      c.options.scope,
		},
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	stream, err := client.JoinCluster(streamCtx, request)
	if err != nil {
		return err
	}

	c.mu.Lock()
	joinCh := make(chan struct{})
	c.closer = cancel
	leaveCh := make(chan struct{})
	c.leaveCh = leaveCh
	c.mu.Unlock()

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
				members := make([]Member, 0, len(response.Membership.Members))
				for _, member := range response.Membership.Members {
					members = append(members, Member{
						ID: MemberID(member.ID.Name),
					})
				}
				membership := Membership{
					Members: members,
				}

				c.mu.Lock()
				c.membership = &membership
				c.mu.Unlock()

				if !joined {
					close(joinCh)
					joined = true
				}

				c.mu.RLock()
				for _, watcher := range c.watchers {
					watcher <- membership
				}
				c.mu.RUnlock()
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

// Watch watches the membership for changes
func (c *Cluster) Watch(ctx context.Context, ch chan<- Membership) error {
	c.mu.Lock()
	watcher := make(chan Membership)
	membership := c.membership
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
				c.mu.Lock()
				watchers := make([]chan<- Membership, 0)
				for _, ch := range c.watchers {
					if ch != watcher {
						watchers = append(watchers, ch)
					}
				}
				c.watchers = watchers
				c.mu.Unlock()
				close(watcher)
			}
		}
	}()
	c.watchers = append(c.watchers, watcher)
	c.mu.Unlock()
	return nil
}

// Close closes the cluster
func (c *Cluster) Close() error {
	c.mu.RLock()
	closer := c.closer
	leaveCh := c.leaveCh
	c.mu.RUnlock()
	timeout := time.Minute
	if c.options.joinTimeout != nil {
		timeout = *c.options.joinTimeout
	}
	if closer != nil {
		closer()
		select {
		case <-leaveCh:
			return nil
		case <-time.After(timeout):
			return errors.New("leave timed out")
		}
	}
	return nil
}

// Membership is a set of cluster members
type Membership struct {
	Members []Member
}

// MemberID is a member identifier
type MemberID string

// Member is a membership group member
type Member struct {
	ID MemberID
}
