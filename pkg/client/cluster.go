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
)

// Cluster manages the cluster membership for a client
type Cluster struct {
	client     *Client
	membership *ClusterMembership
	watchers   []chan<- ClusterMembership
	closer     context.CancelFunc
	leaveCh    chan struct{}
	mu         sync.RWMutex
}

// Membership returns the current cluster membership
func (c *Cluster) Membership() ClusterMembership {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.membership != nil {
		return *c.membership
	}
	return ClusterMembership{}
}

// join joins the cluster
func (c *Cluster) join(ctx context.Context) error {
	if c.client.options.memberID == "" {
		return nil
	}

	client := controllerapi.NewClusterServiceClient(c.client.conn)
	request := &controllerapi.JoinClusterRequest{
		Member: controllerapi.Member{
			ID: controllerapi.MemberId{
				Namespace: c.client.options.namespace,
				Name:      c.client.options.memberID,
			},
			Host: c.client.options.peerHost,
			Port: int32(c.client.options.peerPort),
		},
		GroupID: controllerapi.MembershipGroupId{
			Namespace: c.client.options.namespace,
			Name:      c.client.options.scope,
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
				membership := ClusterMembership{
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
func (c *Cluster) Watch(ctx context.Context, ch chan<- ClusterMembership) error {
	c.mu.Lock()
	watcher := make(chan ClusterMembership)
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
				watchers := make([]chan<- ClusterMembership, 0)
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

// leave leaves the cluster
func (c *Cluster) leave(ctx context.Context) error {
	c.mu.RLock()
	closer := c.closer
	leaveCh := c.leaveCh
	c.mu.RUnlock()
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

// ClusterMembership is a set of cluster members
type ClusterMembership struct {
	Members []Member
}
