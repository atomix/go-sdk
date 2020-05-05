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
	clusterapi "github.com/atomix/api/proto/atomix/cluster"
	"github.com/atomix/go-client/pkg/client/primitive"
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

	var member *Member
	if options.memberID != "" {
		member = &Member{
			ID:   MemberID(options.memberID),
			Host: options.peerHost,
			Port: options.peerPort,
		}
	}

	cluster := &Cluster{
		Namespace: options.namespace,
		Name:      options.scope,
		member:    member,
		conn:      conn,
		options:   options,
		leaveCh:   make(chan struct{}),
		watchers:  make([]chan<- Membership, 0),
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
	Namespace  string
	Name       string
	member     *Member
	conn       *grpc.ClientConn
	options    clusterOptions
	membership *Membership
	watchers   []chan<- Membership
	closer     context.CancelFunc
	leaveCh    chan struct{}
	mu         sync.RWMutex
}

// Member returns the local cluster member
func (c *Cluster) Member() *Member {
	return c.member
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

// serve begins service if necessary
func (c *Cluster) serve(ctx context.Context) error {
	if c.member != nil {
		return primitive.Serve(c.member.Port, c.leaveCh)
	}
	return nil
}

// join joins the cluster
func (c *Cluster) join(ctx context.Context) error {
	err := c.serve(ctx)
	if err != nil {
		return err
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

	client := clusterapi.NewClusterServiceClient(c.conn)
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

				oldMembers := make(map[MemberID]*Member)
				if c.membership != nil {
					for _, member := range c.membership.Members {
						oldMembers[member.ID] = member
					}
				}

				newMembers := make([]*Member, 0, len(response.Members))
				for _, member := range response.Members {
					memberID := MemberID(member.ID.Name)
					oldMember, ok := oldMembers[memberID]
					if ok {
						newMembers = append(newMembers, oldMember)
					} else {
						newMembers = append(newMembers, &Member{
							ID:   memberID,
							Host: member.Host,
							Port: int(member.Port),
						})
					}
				}
				membership := Membership{
					Members: newMembers,
				}

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
	Members []*Member
}

// MemberID is a member identifier
type MemberID string

// Member is a membership group member
type Member struct {
	ID   MemberID
	Host string
	Port int
	conn *grpc.ClientConn
	mu   sync.RWMutex
}

// Connect connects to the member
func (m *Member) Connect() (*grpc.ClientConn, error) {
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		return m.conn, nil
	}

	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", m.Host, m.Port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	m.conn = conn
	return conn, err
}
