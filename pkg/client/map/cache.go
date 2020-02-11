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
	"context"
	"errors"
	"github.com/hashicorp/golang-lru"
	"sync"
)

// newCachingMap returns a decorated map that caches updates to the given map
func newCachingMap(_map Map, size int) (Map, error) {
	cache, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	cachingMap := &cachingMap{
		delegatingMap: newDelegatingMap(_map),
		cache:         cache,
		state: &cacheState{
			waiters: make(map[int64]*sync.Cond),
			mu:      &sync.RWMutex{},
		},
	}
	if err := cachingMap.open(); err != nil {
		return nil, err
	}
	return cachingMap, nil
}

// cachingMap is an implementation of the Map interface that caches entries
type cachingMap struct {
	*delegatingMap
	cancel context.CancelFunc
	cache  *lru.Cache
	state  *cacheState
}

// open opens the map listeners
func (m *cachingMap) open() error {
	ch := make(chan *Event)
	ctx, cancel := context.WithCancel(context.Background())
	m.state.Lock()
	m.cancel = cancel
	m.state.Unlock()
	if err := m.delegatingMap.Watch(ctx, ch, WithReplay()); err != nil {
		return err
	}
	go func() {
		for event := range ch {
			switch event.Type {
			case EventNone:
				m.cache.Add(event.Entry.Key, event.Entry)
			case EventInserted:
				m.cache.Add(event.Entry.Key, event.Entry)
			case EventUpdated:
				m.cache.Add(event.Entry.Key, event.Entry)
			case EventRemoved:
				m.cache.Remove(event.Entry.Key)
			}

			// Wake up goroutines waiting for this update
			m.state.setMaxUpdated(event.Entry.Version)
		}
	}()
	return nil
}

func (m *cachingMap) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry, error) {
	// Put the entry in the map using the underlying map delegate
	entry, err := m.delegatingMap.Put(ctx, key, value, opts...)
	if err != nil {
		return nil, err
	}

	// If the update is successful, record the max seen version
	m.state.setMaxSeen(entry.Version)
	return entry, nil
}

func (m *cachingMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error) {
	// Remove the entry from the map using the underlying map delegate
	entry, err := m.delegatingMap.Remove(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	// If the update is successful, update the max seen version for read-your-writes consistency
	if entry != nil {
		m.state.setMaxSeen(entry.Version)
	}
	return entry, nil
}

func (m *cachingMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
	// Get the current cache state
	m.state.RLock()
	closed := m.state.closed
	current := m.state.isCurrent()
	m.state.RUnlock()

	// If the cache is closed, return an error
	if closed {
		return nil, errors.New("cache closed")
	}

	// If the client write a value at a later point than the current cache point, wait for updates
	// to be propagated to the cache
	if !current {
		// Acquire a write lock again (double checked lock)
		m.state.Lock()

		// Check whether the cache is closed again
		if m.state.closed {
			return nil, errors.New("cache closed")
		}

		// Check the current cache point again before creating a condition
		if !m.state.isCurrent() {
			m.state.awaitUpdate()
		}

		// Check that the cache is not closed once more - it could have been closed during a awaitUpdate() call
		if m.state.closed {
			return nil, errors.New("cache closed")
		}

		// Release the write lock
		m.state.Unlock()
	}

	// If the entry is present in the cache, return it
	if entry, ok := m.cache.Get(key); ok {
		return entry.(*Entry), nil
	}

	// Otherwise, fetch the entry from the underlying map and cache it
	entry, err := m.delegatingMap.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	m.cache.Add(key, entry)
	return entry, nil
}

func (m *cachingMap) Close(ctx context.Context) error {
	m.state.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	m.state.closed = true
	m.state.Unlock()
	return m.delegatingMap.Close(ctx)
}

// cacheState contains the state of the cache
type cacheState struct {
	maxSeen     int64
	maxUpdated  int64
	maxComplete int64
	waiters     map[int64]*sync.Cond
	mu          *sync.RWMutex
	closed      bool
}

// Lock locks the cache state
func (s *cacheState) Lock() {
	s.mu.Lock()
}

// Unlock unlocks the cache state
func (s *cacheState) Unlock() {
	s.mu.Unlock()
}

// RLock read locks the cache state
func (s *cacheState) RLock() {
	s.mu.RLock()
}

// rUnlock read unlocks the cache state
func (s *cacheState) RUnlock() {
	s.mu.RUnlock()
}

// setMaxSeen sets the max seen version
func (s *cacheState) setMaxSeen(seenVersion int64) {
	s.mu.Lock()
	if seenVersion > s.maxSeen {
		s.maxSeen = seenVersion
	}
	s.mu.Unlock()
}

// setMaxUpdated sets the max updated version and awakens waiters waiting for the update
func (s *cacheState) setMaxUpdated(updateVersion int64) {
	s.mu.Lock()
	if updateVersion > s.maxUpdated {
		s.maxUpdated = updateVersion
		for version := s.maxComplete; version <= updateVersion; version++ {
			waiter, ok := s.waiters[version]
			if ok {
				waiter.Broadcast()
			}
			s.maxComplete = version
		}
	}
	s.mu.Unlock()
}

// awaitUpdate waits for the cache state to be propagated
func (s *cacheState) awaitUpdate() {
	maxSeen := s.maxSeen
	waiter, ok := s.waiters[maxSeen]
	if !ok {
		waiter = sync.NewCond(s.mu)
		s.waiters[maxSeen] = waiter
	}
	for s.closed || maxSeen <= s.maxUpdated {
		waiter.Wait()
	}
}

// isCurrent returns a boolean indicating whether the cache is current
func (s *cacheState) isCurrent() bool {
	return s.maxSeen <= s.maxUpdated
}
