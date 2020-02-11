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
	mu     sync.RWMutex
}

// open opens the map listeners
func (m *cachingMap) open() error {
	ch := make(chan *Event)
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()
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

	// If the entry in the cache is still older than the updated entry, update the entry
	// This check is performed because a concurrent event could update the cached entry
	m.mu.Lock()
	prevEntry, ok := m.cache.Get(key)
	if !ok || prevEntry.(*Entry).Version < entry.Version {
		m.cache.Add(key, entry)
	}
	m.mu.Unlock()
	return entry, nil
}

func (m *cachingMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error) {
	// Remove the entry from the map using the underlying map delegate
	entry, err := m.delegatingMap.Remove(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	// If the entry in the cache is still older than the removed entry, remove the entry
	// This check is performed because a concurrent event could update the cached entry
	m.mu.Lock()
	prevEntry, ok := m.cache.Get(key)
	if ok && prevEntry.(*Entry).Version <= entry.Version {
		m.cache.Remove(key)
	}
	m.mu.Unlock()
	return entry, nil
}

func (m *cachingMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
	// If the entry is already in the cache, return it
	m.mu.RLock()
	cachedEntry, ok := m.cache.Get(key)
	m.mu.RUnlock()
	if ok {
		return cachedEntry.(*Entry), nil
	}

	// Otherwise, fetch the entry from the underlying map
	// Note that we do not cache the entry here since it could have been removed, in which
	// case we'd have to maintain tombstones in the cache to compare versions.
	entry, err := m.delegatingMap.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (m *cachingMap) Close(ctx context.Context) error {
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Unlock()
	return m.delegatingMap.Close(ctx)
}
