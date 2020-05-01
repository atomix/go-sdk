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

package _map

import (
	"container/list"
	"context"
	"github.com/atomix/api/proto/atomix/gossip_map"
	primitiveapi "github.com/atomix/api/proto/atomix/primitive"
	"github.com/atomix/go-client/pkg/client/p2p/primitive"
	"github.com/google/uuid"
	"math/rand"
	"sync"
	"time"
)

// NewGossipMap creates a new gossip Map
func NewGossipMap(ctx context.Context, name primitive.Name, peers *primitive.PeerGroup, opts ...GossipMapOption) (Map, error) {
	options := applyGossipMapOptions(opts...)
	m := &gossipMap{
		name:     name,
		peers:    peers,
		options:  options,
		streams:  make(map[primitive.PeerID]gossip_map.GossipMapService_ConnectClient),
		updates:  list.New(),
		watchers: make(map[string]chan<- Event),
		closeCh:  make(chan struct{}),
	}
	err := m.bootstrap(ctx)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// gossipMap is a gossip based peer-to-peer map
type gossipMap struct {
	name      primitive.Name
	peers     *primitive.PeerGroup
	options   gossipMapOptions
	entries   map[string]gossip_map.MapValue
	entriesMu sync.RWMutex
	streams   map[primitive.PeerID]gossip_map.GossipMapService_ConnectClient
	streamsMu sync.RWMutex
	updates   *list.List
	watchers  map[string]chan<- Event
	timestamp uint64
	closeCh   chan struct{}
}

func (m *gossipMap) Name() primitive.Name {
	return m.name
}

// watchPeers watches peers for changes
func (m *gossipMap) watchPeers() error {
	ch := make(chan primitive.PeerGroup)
	err := m.peers.Watch(context.Background(), ch)
	if err != nil {
		return err
	}

	go func() {
		for peers := range ch {
			active := make(map[primitive.PeerID]bool)
			for _, peer := range peers.Peers() {
				m.streamsMu.RLock()
				_, ok := m.streams[peer.ID]
				if !ok {
					conn, err := peer.Connect()
					if err == nil {
						client := gossip_map.NewGossipMapServiceClient(conn)
						stream, err := client.Connect(context.Background())
						if err == nil {
							m.streams[peer.ID] = stream
							_ = stream.Send(&gossip_map.Message{
								Target: primitiveapi.Name{
									Namespace: m.name.Group,
									Name:      m.name.Name,
								},
								Message: &gossip_map.Message_BootstrapRequest{
									BootstrapRequest: &gossip_map.BootstrapRequest{},
								},
							})
						}
					}
				}
				active[peer.ID] = true
				m.streamsMu.RUnlock()
			}
			m.streamsMu.Lock()
			for peer, stream := range m.streams {
				if !active[peer] {
					delete(m.streams, peer)
					_ = stream.CloseSend()
				}
			}
			m.streamsMu.Unlock()
		}
	}()
	return nil
}

// bootstrap bootstraps the map
func (m *gossipMap) bootstrap(ctx context.Context) error {
	getManager().register(m.name, m.handle)
	err := m.watchPeers()
	if err != nil {
		return err
	}
	for _, peer := range m.peers.Peers() {
		conn, err := peer.Connect()
		if err != nil {
			return err
		}
		client := gossip_map.NewGossipMapServiceClient(conn)
		stream, err := client.Connect(ctx)
		if err != nil {
			return err
		}
		m.streamsMu.Lock()
		m.streams[peer.ID] = stream
		m.streamsMu.Unlock()
		_ = stream.Send(&gossip_map.Message{
			Target: primitiveapi.Name{
				Namespace: m.name.Group,
				Name:      m.name.Name,
			},
			Message: &gossip_map.Message_BootstrapRequest{
				BootstrapRequest: &gossip_map.BootstrapRequest{},
			},
		})
	}
	go m.sendUpdates()
	go m.sendAdvertisements()
	return nil
}

func (m *gossipMap) sendUpdates() {
	ticker := time.NewTicker(m.options.gossipPeriod)
	for {
		select {
		case <-ticker.C:
			m.sendUpdate()
		case <-m.closeCh:
			return
		}
	}
}

func (m *gossipMap) sendUpdate() {
	m.entriesMu.Lock()
	updates := make([]*gossip_map.UpdateEntry, 0)
	entry := m.updates.Front()
	for i := 0; i < 100 && entry != nil; i++ {
		updates = append(updates, entry.Value.(*gossip_map.UpdateEntry))
		next := entry.Next()
		m.updates.Remove(entry)
		entry = next
	}
	timestamp := m.timestamp
	m.entriesMu.Unlock()

	m.streamsMu.Lock()
	for _, stream := range m.streams {
		_ = stream.Send(&gossip_map.Message{
			Message: &gossip_map.Message_Update{
				Update: &gossip_map.Update{
					Updates:   updates,
					Timestamp: timestamp,
				},
			},
		})
	}
	m.streamsMu.Unlock()
}

func (m *gossipMap) sendAdvertisements() {
	ticker := time.NewTicker(m.options.antiEntropyPeriod)
	for {
		select {
		case <-ticker.C:
			m.sendAdvertisement()
		case <-m.closeCh:
			return
		}
	}
}

func (m *gossipMap) sendAdvertisement() {
	peers := m.peers.Peers()
	peer := peers[rand.Intn(len(peers))]

	m.streamsMu.RLock()
	stream, ok := m.streams[peer.ID]
	m.streamsMu.RUnlock()
	if !ok {
		return
	}

	m.entriesMu.RLock()
	timestamp := m.timestamp
	entries := make(map[string]*gossip_map.Digest)
	for key, value := range m.entries {
		entries[key] = &value.Digest
	}
	m.entriesMu.RUnlock()

	_ = stream.Send(&gossip_map.Message{
		Message: &gossip_map.Message_AntiEntropyAdvertisement{
			AntiEntropyAdvertisement: &gossip_map.AntiEntropyAdvertisement{
				Digest:    entries,
				Timestamp: timestamp,
			},
		},
	})
}

// handle handles a message
func (m *gossipMap) handle(message *gossip_map.Message, stream gossip_map.GossipMapService_ConnectServer) error {
	switch msg := message.Message.(type) {
	case *gossip_map.Message_BootstrapRequest:
		return m.handleBootstrapRequest(msg.BootstrapRequest, stream)
	case *gossip_map.Message_Update:
		return m.handleUpdate(msg.Update, stream)
	case *gossip_map.Message_UpdateRequest:
		return m.handleUpdateRequest(msg.UpdateRequest, stream)
	case *gossip_map.Message_AntiEntropyAdvertisement:
		return m.handleAntiEntropyAdvertisement(msg.AntiEntropyAdvertisement, stream)
	}
	return nil
}

// handleBootstrapRequest handles a BootstrapRequest
func (m *gossipMap) handleBootstrapRequest(message *gossip_map.BootstrapRequest, stream gossip_map.GossipMapService_ConnectServer) error {
	return nil
}

// handleUpdate handles an Update
func (m *gossipMap) handleUpdate(message *gossip_map.Update, stream gossip_map.GossipMapService_ConnectServer) error {
	return nil
}

// handleUpdateRequest handles an UpdateRequest
func (m *gossipMap) handleUpdateRequest(message *gossip_map.UpdateRequest, stream gossip_map.GossipMapService_ConnectServer) error {
	return nil
}

// handleAntiEntropyAdvertisement handles an AntiEntropyAdvertisement
func (m *gossipMap) handleAntiEntropyAdvertisement(message *gossip_map.AntiEntropyAdvertisement, stream gossip_map.GossipMapService_ConnectServer) error {
	return nil
}

func (m *gossipMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
	m.entriesMu.RLock()
	defer m.entriesMu.RUnlock()
	entry, ok := m.entries[key]
	if ok && !entry.Digest.Tombstone {
		return &Entry{
			Key:   key,
			Value: entry.Value,
		}, nil
	}
	return nil, nil
}

func (m *gossipMap) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry, error) {
	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()
	m.timestamp++
	entry := gossip_map.MapValue{
		Digest: gossip_map.Digest{
			Timestamp: m.timestamp,
		},
		Value: value,
	}
	m.entries[key] = entry
	m.updates.PushBack(&gossip_map.UpdateEntry{
		Key:   key,
		Value: &entry,
	})
	return &Entry{
		Key:   key,
		Value: entry.Value,
	}, nil
}

func (m *gossipMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error) {
	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()
	m.timestamp++
	entry, ok := m.entries[key]
	if !ok {
		return nil, nil
	}
	update := gossip_map.MapValue{
		Digest: gossip_map.Digest{
			Timestamp: m.timestamp,
			Tombstone: true,
		},
	}
	m.entries[key] = update
	m.updates.PushBack(&gossip_map.UpdateEntry{
		Key:   key,
		Value: &update,
	})
	return &Entry{
		Key:   key,
		Value: entry.Value,
	}, nil
}

func (m *gossipMap) Len(ctx context.Context) (int, error) {
	m.entriesMu.RLock()
	defer m.entriesMu.RUnlock()
	return len(m.entries), nil
}

func (m *gossipMap) Clear(ctx context.Context) error {
	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()
	m.timestamp++
	for key := range m.entries {
		_, ok := m.entries[key]
		if ok {
			update := gossip_map.MapValue{
				Digest: gossip_map.Digest{
					Timestamp: m.timestamp,
					Tombstone: true,
				},
			}
			m.entries[key] = update
			m.updates.PushBack(&gossip_map.UpdateEntry{
				Key:   key,
				Value: &update,
			})
		}
	}
	return nil
}

func (m *gossipMap) Entries(ctx context.Context, ch chan<- Entry) error {
	go func() {
		m.entriesMu.RLock()
		defer m.entriesMu.RUnlock()
		for key, value := range m.entries {
			if !value.Digest.Tombstone {
				ch <- Entry{
					Key:   key,
					Value: value.Value,
				}
			}
		}
	}()
	return nil
}

func (m *gossipMap) Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error {
	id := uuid.New().String()
	m.entriesMu.Lock()
	m.watchers[id] = ch
	m.entriesMu.Unlock()
	go func() {
		<-ctx.Done()
		m.entriesMu.Lock()
		delete(m.watchers, id)
		m.entriesMu.Unlock()
	}()
	return nil
}

func (m *gossipMap) Close(ctx context.Context) error {
	close(m.closeCh)
	return nil
}

func (m *gossipMap) Delete(ctx context.Context) error {
	close(m.closeCh)
	return nil
}
