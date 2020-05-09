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

package _map //nolint:golint

import (
	"context"
	"github.com/atomix/go-client/pkg/client/cluster"
	"github.com/atomix/go-client/pkg/client/pb/replica"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type testReplicaProvider struct {
	ch <-chan replica.Set
}

func (p *testReplicaProvider) Watch(ctx context.Context, ch chan<- replica.Set) error {
	go func() {
		for replicas := range p.ch {
			ch <- replicas
		}
	}()
	return nil
}

func TestGossipMap(t *testing.T) {
	name := primitive.NewName("default", "test", "default", "test")

	member1 := cluster.NewLocalMember("member-1", "localhost", 5000)
	err := member1.Serve()
	assert.NoError(t, err)
	replicaCh11 := make(chan replica.Set)
	replicas11, err := replica.NewGroup(replica.GroupID(1), member1.Member, &testReplicaProvider{replicaCh11})
	assert.NoError(t, err)
	replicaCh12 := make(chan replica.Set)
	replicas12, err := replica.NewGroup(replica.GroupID(2), member1.Member, &testReplicaProvider{replicaCh12})
	assert.NoError(t, err)
	replicaCh13 := make(chan replica.Set)
	replicas13, err := replica.NewGroup(replica.GroupID(3), member1.Member, &testReplicaProvider{replicaCh13})
	assert.NoError(t, err)

	replicas1 := []*replica.Group{replicas11, replicas12, replicas13}
	_map1, err := New(context.Background(), name, replicas1)
	assert.NoError(t, err)

	member2 := cluster.NewLocalMember("member-2", "localhost", 5001)
	err = member2.Serve()
	assert.NoError(t, err)
	replicaCh21 := make(chan replica.Set)
	replicas21, err := replica.NewGroup(replica.GroupID(1), member1.Member, &testReplicaProvider{replicaCh21})
	assert.NoError(t, err)
	replicaCh22 := make(chan replica.Set)
	replicas22, err := replica.NewGroup(replica.GroupID(2), member1.Member, &testReplicaProvider{replicaCh22})
	assert.NoError(t, err)
	replicaCh23 := make(chan replica.Set)
	replicas23, err := replica.NewGroup(replica.GroupID(3), member1.Member, &testReplicaProvider{replicaCh23})
	assert.NoError(t, err)

	replicas2 := []*replica.Group{replicas21, replicas22, replicas23}
	_map2, err := New(context.Background(), name, replicas2)
	assert.NoError(t, err)

	replicaCh11 <- replica.Set{
		Term: 1,
		Primary: &replica.Replica{
			ID:     replica.ID(member1.ID),
			Member: member1.Member,
		},
		Backups: []replica.Replica{
			{
				ID:     replica.ID(member2.ID),
				Member: member2.Member,
			},
		},
	}
	replicaCh21 <- replica.Set{
		Term: 1,
		Primary: &replica.Replica{
			ID:     replica.ID(member1.ID),
			Member: member1.Member,
		},
		Backups: []replica.Replica{
			{
				ID:     replica.ID(member2.ID),
				Member: member2.Member,
			},
		},
	}

	replicaCh12 <- replica.Set{
		Term: 1,
		Primary: &replica.Replica{
			ID:     replica.ID(member2.ID),
			Member: member2.Member,
		},
		Backups: []replica.Replica{
			{
				ID:     replica.ID(member1.ID),
				Member: member1.Member,
			},
		},
	}
	replicaCh22 <- replica.Set{
		Term: 1,
		Primary: &replica.Replica{
			ID:     replica.ID(member2.ID),
			Member: member2.Member,
		},
		Backups: []replica.Replica{
			{
				ID:     replica.ID(member1.ID),
				Member: member1.Member,
			},
		},
	}

	replicaCh13 <- replica.Set{
		Term: 1,
		Primary: &replica.Replica{
			ID:     replica.ID(member1.ID),
			Member: member1.Member,
		},
		Backups: []replica.Replica{
			{
				ID:     replica.ID(member2.ID),
				Member: member2.Member,
			},
		},
	}
	replicaCh23 <- replica.Set{
		Term: 1,
		Primary: &replica.Replica{
			ID:     replica.ID(member1.ID),
			Member: member1.Member,
		},
		Backups: []replica.Replica{
			{
				ID:     replica.ID(member2.ID),
				Member: member2.Member,
			},
		},
	}

	ch := make(chan Event)
	err = _map2.Watch(context.Background(), ch)
	assert.NoError(t, err)

	entry, err := _map1.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "bar", string(entry.Value))

	entry, err = _map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "bar", string(entry.Value))

	select {
	case event := <-ch:
		assert.Equal(t, "foo", event.Entry.Key)
		assert.Equal(t, "bar", string(event.Entry.Value))
	case <-time.After(5 * time.Second):
		t.Log("Timed out waiting for 'foo' to propagate")
		t.FailNow()
	}

	entry, err = _map2.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "bar", string(entry.Value))

	err = _map1.Close(context.Background())
	assert.NoError(t, err)
	err = _map2.Close(context.Background())
	assert.NoError(t, err)
}
