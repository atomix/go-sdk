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

package _map //nolint:golint

import (
	"context"
	"github.com/atomix/go-client/pkg/client/cluster"
	"github.com/atomix/go-client/pkg/client/gossip/peer"
	"github.com/atomix/go-client/pkg/client/primitive"
	times "github.com/atomix/go-client/pkg/client/time"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type testPeerProvider struct {
	ch <-chan peer.Set
}

func (p *testPeerProvider) Watch(ctx context.Context, ch chan<- peer.Set) error {
	go func() {
		for peers := range p.ch {
			ch <- peers
		}
	}()
	return nil
}

func TestGossipMap(t *testing.T) {
	name := primitive.NewName("default", "test", "default", "test")

	member1 := cluster.NewLocalMember("member-1", "localhost", 5000)
	err := member1.Serve()
	assert.NoError(t, err)
	peerCh1 := make(chan peer.Set)
	peers1, err := peer.NewGroup(member1.Member, &testPeerProvider{peerCh1})
	assert.NoError(t, err)

	_map1, err := NewMap(context.Background(), name, peers1, WithClock(times.NewLogicalClock()))
	assert.NoError(t, err)

	member2 := cluster.NewLocalMember("member-2", "localhost", 5001)
	err = member2.Serve()
	assert.NoError(t, err)
	peerCh2 := make(chan peer.Set)
	peers2, err := peer.NewGroup(member2.Member, &testPeerProvider{peerCh2})
	assert.NoError(t, err)

	_map2, err := NewMap(context.Background(), name, peers2, WithClock(times.NewLogicalClock()))
	assert.NoError(t, err)

	peerCh1 <- peer.Set{
		peer.Peer{
			ID:     peer.ID(member2.ID),
			Member: member2.Member,
		},
	}

	peerCh2 <- peer.Set{
		peer.Peer{
			ID:     peer.ID(member1.ID),
			Member: member1.Member,
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
