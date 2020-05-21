// Copyright 2020-present Open Networking Foundation.
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

package peer

import (
	"context"
	"errors"
	"fmt"
	clusterapi "github.com/atomix/api/proto/atomix/membership"
	"google.golang.org/grpc"
	"io"
	"sync"
	"time"
)

// NewGroup creates a new peer group
func NewGroup(address string, opts ...Option) (*Group, error) {
	options := applyOptions(opts...)

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	var member *Member
	if options.memberID != "" {
		services := options.services
		if services == nil {
			services = []Service{}
		}
		member = NewMember(ID(options.memberID), options.peerHost, options.peerPort, services...)
	}

	cluster := &Group{
		Namespace: options.namespace,
		Name:      options.scope,
		member:    member,
		peers:     make(Peers, 0),
		peersByID: make(map[ID]*Peer),
		conn:      conn,
		options:   *options,
		leaveCh:   make(chan struct{}),
		watchers:  make([]chan<- Peers, 0),
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

// Group manages the peer group for a client
type Group struct {
	Namespace string
	Name      string
	member    *Member
	conn      *grpc.ClientConn
	options   options
	peers     Peers
	peersByID map[ID]*Peer
	watchers  []chan<- Peers
	closer    context.CancelFunc
	leaveCh   chan struct{}
	mu        sync.RWMutex
}

// Member returns the local group member
func (c *Group) Member() *Peer {
	return c.member.Peer
}

// Peer returns a peer by ID
func (c *Group) Peer(id ID) *Peer {
	return c.peersByID[id]
}

// Peers returns the current group peers
func (c *Group) Peers() Peers {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.peers != nil {
		return c.peers
	}
	return Peers{}
}

// join joins the cluster
func (c *Group) join(ctx context.Context) error {
	if c.member != nil {
		err := c.member.serve()
		if err != nil {
			return nil
		}
	}

	var member *clusterapi.Member
	if c.member != nil {
		member = &clusterapi.Member{
			ID: clusterapi.MemberId{
				Namespace: c.options.namespace,
				Name:      c.options.memberID,
			},
			Host: c.member.Host,
			Port: int32(c.member.Port),
		}
	}

	client := clusterapi.NewMembershipServiceClient(c.conn)
	request := &clusterapi.JoinClusterRequest{
		Member: member,
		ClusterID: clusterapi.ClusterId{
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
	c.mu.Unlock()

	go func() {
		joined := false
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				close(c.leaveCh)
				return
			} else if err != nil {
				fmt.Println(err)
				close(c.leaveCh)
				return
			} else {
				c.mu.Lock()

				oldPeers := make(map[ID]*Peer)
				if c.peers != nil {
					for _, peer := range c.peers {
						oldPeers[peer.ID] = peer
					}
				}

				newPeers := make([]*Peer, 0, len(response.Members))
				for _, member := range response.Members {
					memberID := ID(member.ID.Name)
					oldMember, ok := oldPeers[memberID]
					if ok {
						newPeers = append(newPeers, oldMember)
					} else {
						newPeers = append(newPeers, NewPeer(memberID, member.Host, int(member.Port)))
					}
				}

				c.peers = newPeers
				peersByID := make(map[ID]*Peer)
				for _, peer := range newPeers {
					peersByID[peer.ID] = peer
				}
				c.peersByID = peersByID
				c.mu.Unlock()

				if !joined {
					close(joinCh)
					joined = true
				}

				c.mu.RLock()
				for _, watcher := range c.watchers {
					watcher <- newPeers
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

// Watch watches the peers for changes
func (c *Group) Watch(ctx context.Context, ch chan<- Peers) error {
	c.mu.Lock()
	watcher := make(chan Peers)
	peers := c.peers
	go func() {
		if peers != nil {
			ch <- peers
		}
		for {
			select {
			case peers, ok := <-watcher:
				if !ok {
					return
				}
				ch <- peers
			case <-ctx.Done():
				c.mu.Lock()
				watchers := make([]chan<- Peers, 0)
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
func (c *Group) Close() error {
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

// Peers is a set of peers
type Peers []*Peer
