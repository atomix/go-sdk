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
	"github.com/atomix/api/proto/atomix/gossip/headers"
	mapapi "github.com/atomix/api/proto/atomix/gossip/map"
	"github.com/atomix/api/proto/atomix/membership"
	primitiveapi "github.com/atomix/api/proto/atomix/primitive"
	"github.com/atomix/api/proto/atomix/protocol"
	"github.com/atomix/go-client/pkg/client/gossip/peer"
	"github.com/atomix/go-client/pkg/client/primitive"
	times "github.com/atomix/go-client/pkg/client/time"
	"github.com/google/uuid"
	"math/rand"
	"sync"
	"time"
)

// NewMap creates a new gossip Map
func NewMap(ctx context.Context, name primitive.Name, peers *peer.Group, opts ...Option) (Map, error) {
	options := applyGossipMapOptions(opts...)
	m := &gossipMap{
		name:     name,
		options:  options,
		group:    peers,
		peers:    make(map[peer.ID]*gossipMapPeer),
		entries:  make(map[string]timestampedEntry),
		watchers: make(map[string]chan<- Event),
		eventCh:  make(chan Event),
		clock:    options.clock,
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
	name       primitive.Name
	options    gossipMapOptions
	group      *peer.Group
	peers      map[peer.ID]*gossipMapPeer
	peersMu    sync.RWMutex
	entries    map[string]timestampedEntry
	entriesMu  sync.RWMutex
	watchers   map[string]chan<- Event
	watchersMu sync.RWMutex
	eventCh    chan Event
	clock      times.Clock
	closeCh    chan struct{}
}

func (m *gossipMap) Name() primitive.Name {
	return m.name
}

// watchPeers watches peers for changes
func (m *gossipMap) watchPeers() error {
	ch := make(chan peer.Set)
	ctx, cancel := context.WithCancel(context.Background())
	err := m.group.Watch(ctx, ch)
	if err != nil {
		return err
	}

	go func() {
		<-m.closeCh
		cancel()
	}()

	go func() {
		for peers := range ch {
			active := make(map[peer.ID]bool)
			for _, peer := range peers {
				m.peersMu.RLock()
				_, ok := m.peers[peer.ID]
				m.peersMu.RUnlock()
				if !ok {
					mapPeer, err := newGossipMapPeer(m.name, m.group, &peer, m.clock, m.options)
					if err == nil {
						m.peersMu.Lock()
						if _, ok := m.peers[peer.ID]; !ok {
							m.peers[peer.ID] = mapPeer
						} else {
							_ = mapPeer.close()
						}
						m.peersMu.Unlock()
					}
				}
				active[peer.ID] = true
			}
			m.peersMu.Lock()
			for peerID, peer := range m.peers {
				if !active[peerID] {
					delete(m.peers, peerID)
					peer.close()
				}
			}
			m.peersMu.Unlock()
		}
	}()
	return nil
}

// bootstrap bootstraps the map
func (m *gossipMap) bootstrap(ctx context.Context) error {
	getManager(m.group.Member.ID).register(m.name, m.handle)
	err := m.watchPeers()
	if err != nil {
		return err
	}
	for _, peer := range m.group.Peers() {
		m.peersMu.RLock()
		_, ok := m.peers[peer.ID]
		m.peersMu.RUnlock()
		if !ok {
			mapPeer, err := newGossipMapPeer(m.name, m.group, peer, m.clock, m.options)
			if err == nil {
				m.peersMu.Lock()
				if _, ok := m.peers[peer.ID]; !ok {
					m.peers[peer.ID] = mapPeer
				} else {
					_ = mapPeer.close()
				}
				m.peersMu.Unlock()
			}
		}
	}
	go m.startSendAdvertisements()
	go m.startPurgeTombstones()
	go m.startPublishEvents()
	return nil
}

func (m *gossipMap) startPurgeTombstones() {
	ticker := time.NewTicker(m.options.tombstonePurgePeriod)
	for {
		select {
		case <-ticker.C:
			m.purgeTombstones()
		case <-m.closeCh:
			return
		}
	}
}

func (m *gossipMap) purgeTombstones() {
	m.entriesMu.Lock()
	now := time.Now()
	for key, entry := range m.entries {
		if entry.value.Digest.Tombstone && now.Sub(entry.timestamp) > m.options.tombstonePurgePeriod {
			delete(m.entries, key)
		}
	}
	m.entriesMu.Unlock()
}

func (m *gossipMap) startSendAdvertisements() {
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
	peers := m.group.Peers()
	if len(peers) == 0 {
		return
	}

	m.peersMu.RLock()
	peer, ok := m.peers[peers[rand.Intn(len(peers))].ID]
	m.peersMu.RUnlock()
	if !ok {
		return
	}

	digest := make(map[string]mapapi.Digest)
	m.entriesMu.RLock()
	for key, entry := range m.entries {
		digest[key] = entry.value.Digest
	}
	m.entriesMu.RUnlock()
	peer.sendAdvertisement(digest)
}

func (m *gossipMap) startPublishEvents() {
	for {
		select {
		case event := <-m.eventCh:
			m.publishEvent(event)
		case <-m.closeCh:
			return
		}
	}
}

func (m *gossipMap) publishEvent(event Event) {
	m.watchersMu.RLock()
	for _, watcher := range m.watchers {
		watcher <- event
	}
	m.watchersMu.RUnlock()
}

// handle handles a message
func (m *gossipMap) handle(message *mapapi.Message, stream mapapi.GossipMapService_ConnectServer) error {
	m.peersMu.RLock()
	peer, ok := m.peers[peer.ID(message.Header.Source.Name)]
	m.peersMu.RUnlock()
	if !ok {
		return nil
	}

	switch msg := message.Message.(type) {
	case *mapapi.Message_Update:
		return m.handleUpdate(peer, msg.Update)
	case *mapapi.Message_UpdateRequest:
		return m.handleUpdateRequest(peer, msg.UpdateRequest)
	case *mapapi.Message_AntiEntropyAdvertisement:
		return m.handleAntiEntropyAdvertisement(peer, msg.AntiEntropyAdvertisement)
	}
	return nil
}

// handleUpdate handles an Update
func (m *gossipMap) handleUpdate(peer *gossipMapPeer, message *mapapi.Update) error {
	timestamp := m.clock.New()
	err := timestamp.Unmarshal(message.Timestamp)
	if err != nil {
		return err
	}
	m.clock.Update(timestamp)
	for key, update := range message.Updates {
		m.entriesMu.Lock()
		entry, ok := m.entries[key]
		if !ok && !update.Digest.Tombstone {
			m.entries[key] = timestampedEntry{
				timestamp: time.Now(),
				value:     update,
			}
			m.eventCh <- Event{
				Type: EventInserted,
				Entry: Entry{
					Key:   key,
					Value: update.Value,
				},
			}
		} else if ok {
			updateTimestamp := m.clock.New()
			err := updateTimestamp.Unmarshal(update.Digest.Timestamp)
			if err != nil {
				return err
			}
			entryTimestamp := m.clock.New()
			err = entryTimestamp.Unmarshal(entry.value.Digest.Timestamp)
			if err != nil {
				return err
			}
			if updateTimestamp.GreaterThan(entryTimestamp) {
				m.entries[key] = timestampedEntry{
					timestamp: time.Now(),
					value:     update,
				}

				var eventType EventType
				if entry.value.Digest.Tombstone && !update.Digest.Tombstone {
					eventType = EventInserted
				} else if !entry.value.Digest.Tombstone && update.Digest.Tombstone {
					eventType = EventRemoved
				} else if !entry.value.Digest.Tombstone && !update.Digest.Tombstone {
					eventType = EventUpdated
				} else {
					continue
				}

				m.eventCh <- Event{
					Type: eventType,
					Entry: Entry{
						Key:   key,
						Value: update.Value,
					},
				}
			}
		}
		m.entriesMu.Unlock()
	}
	return nil
}

// handleUpdateRequest handles an UpdateRequest
func (m *gossipMap) handleUpdateRequest(peer *gossipMapPeer, message *mapapi.UpdateRequest) error {
	timestamp := m.clock.New()
	err := timestamp.Unmarshal(message.Timestamp)
	if err != nil {
		return err
	}
	m.clock.Update(timestamp)

	for _, key := range message.Keys {
		m.entriesMu.RLock()
		entry, ok := m.entries[key]
		m.entriesMu.RUnlock()
		if ok {
			peer.enqueueUpdate(&mapapi.UpdateEntry{
				Key:   key,
				Value: &entry.value,
			})
		}
	}
	return nil
}

// handleAntiEntropyAdvertisement handles an AntiEntropyAdvertisement
func (m *gossipMap) handleAntiEntropyAdvertisement(peer *gossipMapPeer, message *mapapi.AntiEntropyAdvertisement) error {
	timestamp := m.clock.New()
	err := timestamp.Unmarshal(message.Timestamp)
	if err != nil {
		return err
	}
	timestamp = m.clock.Update(timestamp)

	requests := make([]string, 0)
	for key, digest := range message.Digest {
		m.entriesMu.RLock()
		entry, ok := m.entries[key]
		m.entriesMu.RUnlock()

		if !ok {
			requests = append(requests, key)
		} else {
			digestTimestamp := m.clock.New()
			err := digestTimestamp.Unmarshal(digest.Timestamp)
			if err != nil {
				return err
			}
			entryTimestamp := m.clock.New()
			err = entryTimestamp.Unmarshal(entry.value.Digest.Timestamp)
			if err != nil {
				return err
			}
			if digestTimestamp.GreaterThan(entryTimestamp) {
				requests = append(requests, key)
			} else if digestTimestamp.LessThan(entryTimestamp) {
				peer.enqueueUpdate(&mapapi.UpdateEntry{
					Key:   key,
					Value: &entry.value,
				})
			}
		}
	}

	if len(requests) > 0 {
		err = peer.sendRequests(requests)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *gossipMap) enqueueUpdate(update *mapapi.UpdateEntry) {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()
	for _, peer := range m.peers {
		peer.enqueueUpdate(update)
	}
}

func (m *gossipMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
	m.entriesMu.RLock()
	defer m.entriesMu.RUnlock()
	entry, ok := m.entries[key]
	if ok && !entry.value.Digest.Tombstone {
		return &Entry{
			Key:   key,
			Value: entry.value.Value,
		}, nil
	}
	return nil, nil
}

func (m *gossipMap) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry, error) {
	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()
	timestamp, err := m.clock.Increment().Marshal()
	if err != nil {
		return nil, err
	}
	entry, ok := m.entries[key]
	update := mapapi.MapValue{
		Digest: mapapi.Digest{
			Timestamp: timestamp,
		},
		Value: value,
	}
	m.entries[key] = timestampedEntry{
		timestamp: time.Now(),
		value:     update,
	}

	if !ok || entry.value.Digest.Tombstone {
		m.eventCh <- Event{
			Type: EventInserted,
			Entry: Entry{
				Key:   key,
				Value: value,
			},
		}
	} else if ok && !entry.value.Digest.Tombstone {
		m.eventCh <- Event{
			Type: EventUpdated,
			Entry: Entry{
				Key:   key,
				Value: value,
			},
		}
	}

	go m.enqueueUpdate(&mapapi.UpdateEntry{
		Key:   key,
		Value: &update,
	})
	return &Entry{
		Key:   key,
		Value: update.Value,
	}, nil
}

func (m *gossipMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error) {
	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()
	timestamp, err := m.clock.Increment().Marshal()
	if err != nil {
		return nil, err
	}

	entry, ok := m.entries[key]
	if !ok || entry.value.Digest.Tombstone {
		return nil, nil
	}

	update := mapapi.MapValue{
		Digest: mapapi.Digest{
			Timestamp: timestamp,
			Tombstone: true,
		},
	}
	m.entries[key] = timestampedEntry{
		timestamp: time.Now(),
		value:     update,
	}

	m.eventCh <- Event{
		Type: EventRemoved,
		Entry: Entry{
			Key:   key,
			Value: entry.value.Value,
		},
	}

	go m.enqueueUpdate(&mapapi.UpdateEntry{
		Key:   key,
		Value: &update,
	})
	return &Entry{
		Key:   key,
		Value: entry.value.Value,
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
	timestamp, err := m.clock.Increment().Marshal()
	if err != nil {
		return err
	}
	for key := range m.entries {
		_, ok := m.entries[key]
		if ok {
			update := mapapi.MapValue{
				Digest: mapapi.Digest{
					Timestamp: timestamp,
					Tombstone: true,
				},
			}
			m.entries[key] = timestampedEntry{
				timestamp: time.Now(),
				value:     update,
			}
			go m.enqueueUpdate(&mapapi.UpdateEntry{
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
		for key, entry := range m.entries {
			if !entry.value.Digest.Tombstone {
				ch <- Entry{
					Key:   key,
					Value: entry.value.Value,
				}
			}
		}
	}()
	return nil
}

func (m *gossipMap) Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error {
	id := uuid.New().String()
	m.watchersMu.Lock()
	m.watchers[id] = ch
	m.watchersMu.Unlock()
	go func() {
		<-ctx.Done()
		m.watchersMu.Lock()
		delete(m.watchers, id)
		m.watchersMu.Unlock()
	}()
	return nil
}

func (m *gossipMap) Close(ctx context.Context) error {
	m.peersMu.RLock()
	for _, peer := range m.peers {
		_ = peer.close()
	}
	m.peersMu.RUnlock()
	close(m.closeCh)
	return nil
}

func (m *gossipMap) Delete(ctx context.Context) error {
	close(m.closeCh)
	return nil
}

func newGossipMapPeer(name primitive.Name, group *peer.Group, peer *peer.Peer, clock times.Clock, options gossipMapOptions) (*gossipMapPeer, error) {
	conn, err := peer.Connect()
	if err != nil {
		return nil, err
	}
	client := mapapi.NewGossipMapServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	mapPeer := &gossipMapPeer{
		name:        name,
		options:     options,
		group:       group,
		peer:        peer,
		stream:      stream,
		updates:     list.New(),
		overflowLen: 100,
		clock:       clock,
		cancel:      cancel,
		closeCh:     make(chan struct{}),
	}
	go mapPeer.start()
	return mapPeer, nil
}

// gossipMapPeer is a gossip map peer
type gossipMapPeer struct {
	name        primitive.Name
	options     gossipMapOptions
	group       *peer.Group
	peer        *peer.Peer
	stream      mapapi.GossipMapService_ConnectClient
	updates     *list.List
	overflow    bool
	overflowLen int
	clock       times.Clock
	cancel      context.CancelFunc
	closeCh     chan struct{}
	mu          sync.RWMutex
}

func (p *gossipMapPeer) start() {
	go p.processUpdates()
}

func (p *gossipMapPeer) enqueueUpdate(update *mapapi.UpdateEntry) {
	p.mu.Lock()
	p.updates.PushBack(update)
	if p.updates.Len() == p.overflowLen {
		p.overflow = true
		p.sendUpdates()
	}
	p.mu.Unlock()
}

func (p *gossipMapPeer) processUpdates() {
	ticker := time.NewTicker(p.options.gossipPeriod)
	for {
		select {
		case <-ticker.C:
			p.mu.Lock()
			if p.overflow {
				p.overflow = false
			} else if p.updates.Len() > 0 {
				p.sendUpdates()
			}
			p.mu.Unlock()
		case <-p.closeCh:
			return
		}
	}
}

func (p *gossipMapPeer) sendUpdates() {
	updates := make(map[string]mapapi.MapValue)
	entry := p.updates.Front()
	for entry != nil {
		update := entry.Value.(*mapapi.UpdateEntry)
		updates[update.Key] = *update.Value
		next := entry.Next()
		p.updates.Remove(entry)
		entry = next
	}

	timestamp, err := p.clock.Get().Marshal()
	if err != nil {
		return
	}
	_ = p.stream.Send(&mapapi.Message{
		Header: headers.MessageHeader{
			Protocol: protocol.ProtocolId{
				Namespace: p.name.Namespace,
				Name:      p.name.Protocol,
			},
			Primitive: primitiveapi.PrimitiveId{
				Namespace: p.name.Scope,
				Name:      p.name.Name,
			},
			Source: membership.MemberId{
				Name: string(p.group.Member.ID),
			},
		},
		Message: &mapapi.Message_Update{
			Update: &mapapi.Update{
				Updates:   updates,
				Timestamp: timestamp,
			},
		},
	})
}

func (p *gossipMapPeer) sendAdvertisement(digest map[string]mapapi.Digest) error {
	timestamp, err := p.clock.Get().Marshal()
	if err != nil {
		return err
	}
	return p.stream.Send(&mapapi.Message{
		Header: headers.MessageHeader{
			Protocol: protocol.ProtocolId{
				Namespace: p.name.Namespace,
				Name:      p.name.Protocol,
			},
			Primitive: primitiveapi.PrimitiveId{
				Namespace: p.name.Scope,
				Name:      p.name.Name,
			},
			Source: membership.MemberId{
				Name: string(p.group.Member.ID),
			},
		},
		Message: &mapapi.Message_AntiEntropyAdvertisement{
			AntiEntropyAdvertisement: &mapapi.AntiEntropyAdvertisement{
				Digest:    digest,
				Timestamp: timestamp,
			},
		},
	})
}

func (p *gossipMapPeer) sendRequests(requests []string) error {
	timestampBytes, err := p.clock.Get().Marshal()
	if err != nil {
		return err
	}
	return p.stream.Send(&mapapi.Message{
		Header: headers.MessageHeader{
			Protocol: protocol.ProtocolId{
				Namespace: p.name.Namespace,
				Name:      p.name.Protocol,
			},
			Primitive: primitiveapi.PrimitiveId{
				Namespace: p.name.Scope,
				Name:      p.name.Name,
			},
			Source: membership.MemberId{
				Name: string(p.group.Member.ID),
			},
		},
		Message: &mapapi.Message_UpdateRequest{
			UpdateRequest: &mapapi.UpdateRequest{
				Keys:      requests,
				Timestamp: timestampBytes,
			},
		},
	})
}

func (p *gossipMapPeer) close() error {
	close(p.closeCh)
	p.cancel()
	return p.stream.CloseSend()
}

type timestampedEntry struct {
	timestamp time.Time
	value     mapapi.MapValue
}
