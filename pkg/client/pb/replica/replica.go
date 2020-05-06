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

package replica

import (
	"context"
	"github.com/atomix/go-client/pkg/client/cluster"
	"github.com/google/uuid"
	"sync"
)

// NewGroup creates a new replica group
func NewGroup(id GroupID, member *cluster.Member, provider Provider) (*Group, error) {
	var local *Replica
	if member != nil {
		local = &Replica{
			ID:     ID(member.ID),
			Member: member,
		}
	}
	group := &Group{
		ID:           id,
		local:        local,
		backups:      make([]*Replica, 0),
		replicas:     make([]*Replica, 0),
		replicasByID: make(map[ID]*Replica),
		watchers:     make(map[string]chan<- Set),
	}
	ch := make(chan Set)
	err := provider.Watch(context.Background(), ch)
	if err != nil {
		return nil, err
	}
	go func() {
		for replicas := range ch {
			group.update(replicas)
		}
	}()
	return group, nil
}

// ID is a replica identifier
type ID string

// GroupID is a replica group identifier
type GroupID uint64

// Term is a monotonically increasing epoch
type Term uint64

// Replica is a replica
type Replica struct {
	ID ID
	*cluster.Member
}

// Set is the set of replicas in a group
type Set struct {
	Term    Term
	Primary *Replica
	Backups []Replica
}

// Group is a primitive replica group
type Group struct {
	ID           GroupID
	term         Term
	local        *Replica
	primary      *Replica
	backups      []*Replica
	replicas     []*Replica
	replicasByID map[ID]*Replica
	watchers     map[string]chan<- Set
	mu           sync.RWMutex
}

// Local returns the local replica
func (g *Group) Local() *Replica {
	return g.local
}

// Term returns the term
func (g *Group) Term() Term {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.term
}

// Primary returns the primary replica
func (g *Group) Primary() *Replica {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.primary
}

// Backups returns the set of backups in the group
func (g *Group) Backups() []*Replica {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.backups
}

// Replicas returns the set of replicas in the group
func (g *Group) Replicas() []*Replica {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.replicas
}

// Replica returns a replica by ID
func (g *Group) Replica(id ID) *Replica {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.replicasByID[id]
}

// update updates the replicas in the peer group
func (g *Group) update(replicas Set) {
	g.mu.Lock()
	defer g.mu.Unlock()

	oldReplicas := g.replicasByID

	var newPrimary *Replica
	newBackups := make([]*Replica, 0)
	newReplicas := make([]*Replica, 0)
	newReplicasByID := make(map[ID]*Replica)
	if replicas.Primary != nil {
		replica, ok := oldReplicas[replicas.Primary.ID]
		if !ok {
			replica = replicas.Primary
		}
		newPrimary = replica
		newReplicas = append(newReplicas, replica)
	}

	for _, newReplica := range replicas.Backups {
		replica, ok := oldReplicas[newReplica.ID]
		if !ok {
			replica = &newReplica
		}
		newBackups = append(newBackups, replica)
		newReplicas = append(newReplicas, replica)
		newReplicasByID[replica.ID] = replica
	}

	g.term = replicas.Term
	g.primary = newPrimary
	g.backups = newBackups
	g.replicas = newReplicas
	g.replicasByID = newReplicasByID

	for _, watcher := range g.watchers {
		watcher <- replicas
	}
}

// watch watches the replicas in the group
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

// Provider is a replica provider for replica groups
type Provider interface {
	Watch(context.Context, chan<- Set) error
}
