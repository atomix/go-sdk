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

package peer

import (
	"context"
	"github.com/atomix/go-client/pkg/client/cluster"
	"github.com/google/uuid"
	"sync"
)

// NewGroup creates a new peer group
func NewGroup(member *cluster.Member, provider Provider) (*Group, error) {
	group := &Group{
		Member:    member,
		peersByID: make(map[ID]*Peer),
		peers:     make([]*Peer, 0),
		watchers:  make(map[string]chan<- Set),
	}
	ch := make(chan Set)
	err := provider.Watch(context.Background(), ch)
	if err != nil {
		return nil, err
	}
	go func() {
		for peers := range ch {
			group.update(peers)
		}
	}()
	return group, nil
}

// ID is a peer identifier
type ID string

// Peer is a peer
type Peer struct {
	ID ID
	*cluster.Member
}

// Set is the set of peers in a group
type Set []Peer

// Group is a primitive peer group
type Group struct {
	Member    *cluster.Member
	peersByID map[ID]*Peer
	peers     []*Peer
	watchers  map[string]chan<- Set
	mu        sync.RWMutex
}

// Peers returns the set of peers in the group
func (g *Group) Peers() []*Peer {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.peers
}

// Peer returns a peer by ID
func (g *Group) Peer(id ID) *Peer {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.peersByID[id]
}

// update updates the peers in the peer group
func (g *Group) update(peers Set) {
	g.mu.Lock()
	defer g.mu.Unlock()

	oldPeers := g.peersByID
	newPeers := make([]*Peer, 0)
	newPeersByID := make(map[ID]*Peer)
	for _, newPeer := range peers {
		peer, ok := oldPeers[newPeer.ID]
		if !ok {
			peer = &newPeer
		}
		newPeers = append(newPeers, peer)
		newPeersByID[peer.ID] = peer
	}
	g.peers = newPeers
	g.peersByID = newPeersByID

	for _, watcher := range g.watchers {
		watcher <- peers
	}
}

// watch watches the peers in the group
func (g *Group) Watch(ctx context.Context, ch chan<- Set) error {
	id := uuid.New().String()
	g.mu.Lock()
	g.watchers[id] = ch
	g.mu.Unlock()
	go func() {
		<-ctx.Done()
		g.mu.Lock()
		delete(g.watchers, id)
		g.mu.Unlock()
	}()
	return nil
}

// Provider is a peer provider for peer groups
type Provider interface {
	Watch(context.Context, chan<- Set) error
}
