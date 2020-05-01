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

package primitive

import (
	"context"
	"github.com/atomix/go-client/pkg/client/cluster"
	"sync"
)

// NewPeerGroup creates a new peer group
func NewPeerGroup(provider PeerProvider) (*PeerGroup, error) {
	group := &PeerGroup{
		peersByID: make(map[PeerID]*Peer),
		peers:     make([]*Peer, 0),
		watchers:  make([]chan<- PeerGroup, 0),
	}
	ch := make(chan PeerSet)
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

// PeerProvider is a peer provider for peer groups
type PeerProvider interface {
	Watch(context.Context, chan<- PeerSet) error
}

// PeerSet is the set of peers in a group
type PeerSet []Peer

// PeerGroup is a primitive peer group
type PeerGroup struct {
	peersByID map[PeerID]*Peer
	peers     []*Peer
	watchers  []chan<- PeerGroup
	mu        sync.RWMutex
}

// Peers returns the set of peers in the group
func (g *PeerGroup) Peers() []*Peer {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.peers
}

// Peer returns a peer by ID
func (g *PeerGroup) Peer(id PeerID) *Peer {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.peersByID[id]
}

// update updates the peers in the peer group
func (g *PeerGroup) update(peers PeerSet) {
	g.mu.Lock()
	defer g.mu.Unlock()

	oldPeers := g.peersByID
	newPeers := make([]*Peer, 0)
	newPeersByID := make(map[PeerID]*Peer)
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
}

// watch watches the peers in the group
func (g *PeerGroup) Watch(ctx context.Context, ch chan<- PeerGroup) error {

}

// PeerID is a peer identifier
type PeerID string

// Peer is a peer
type Peer struct {
	ID PeerID
	*cluster.Member
}
