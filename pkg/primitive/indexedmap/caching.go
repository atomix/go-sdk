// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/util/cache"
	"io"
	"sort"
	"sync"
)

func newCachingIndexedMap(ctx context.Context, m IndexedMap[string, []byte], size int) (IndexedMap[string, []byte], error) {
	if size == 0 {
		entries := cache.NewUnsafeKeyValueMirror[string, *Entry[string, []byte]]()
		indexes := cache.NewUnsafeKeyValueMirror[Index, *Entry[string, []byte]]()
		mm := &mirroredIndexedMap{
			cachingIndexedMap: &cachingIndexedMap{
				IndexedMap: m,
				entries:    entries,
				indexes:    indexes,
			},
			entries: entries,
			indexes: indexes,
		}
		if err := mm.open(ctx); err != nil {
			return nil, err
		}
		return mm, nil
	}

	entries, err := cache.NewUnsafeKeyValueLRU[string, *Entry[string, []byte]](size)
	if err != nil {
		return nil, err
	}
	indexes, err := cache.NewUnsafeKeyValueLRU[Index, *Entry[string, []byte]](size)
	if err != nil {
		return nil, err
	}
	cm := &cachedIndexedMap{
		cachingIndexedMap: &cachingIndexedMap{
			IndexedMap: m,
			entries:    entries,
			indexes:    indexes,
		},
	}
	if err := cm.open(); err != nil {
		return nil, err
	}
	return cm, nil
}

type cachedIndexedMap struct {
	*cachingIndexedMap
}

func (m *cachedIndexedMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry[string, []byte], error) {
	if entry, ok := m.entries.Load(key); ok {
		return entry, nil
	}

	entry, err := m.IndexedMap.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	m.update(entry)
	return entry, nil
}

func (m *cachedIndexedMap) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry[string, []byte], error) {
	if entry, ok := m.indexes.Load(index); ok {
		return entry, nil
	}

	entry, err := m.IndexedMap.GetIndex(ctx, index, opts...)
	if err != nil {
		return nil, err
	}
	m.update(entry)
	return entry, nil
}

type mirroredIndexedMap struct {
	*cachingIndexedMap
	entries *cache.UnsafeKeyValueMirror[string, *Entry[string, []byte]]
	indexes *cache.UnsafeKeyValueMirror[Index, *Entry[string, []byte]]
}

func (m *mirroredIndexedMap) open(ctx context.Context) error {
	entries, err := m.IndexedMap.List(ctx)
	if err != nil {
		return err
	}
	for {
		entry, err := entries.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		m.update(entry)
	}
	return m.cachingIndexedMap.open()
}

func (m *mirroredIndexedMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry[string, []byte], error) {
	if entry, ok := m.entries.Load(key); ok {
		return entry, nil
	}
	return nil, errors.NewNotFound("key %s not found", key)
}

func (m *mirroredIndexedMap) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry[string, []byte], error) {
	if entry, ok := m.indexes.Load(index); ok {
		return entry, nil
	}
	return nil, errors.NewNotFound("index %d not found", index)
}

func (m *mirroredIndexedMap) Entries(ctx context.Context) (EntryStream[string, []byte], error) {
	m.mu.RLock()
	mirror := m.entries.Copy()
	m.mu.RUnlock()
	entries := make([]*Entry[string, []byte], 0, len(mirror))
	for _, entry := range mirror {
		entries = append(entries, entry)
	}
	sort.Slice(len(entries), func(i, j int) bool {
		return entries[i].Index < entries[j].Index
	})
	return stream.NewSliceStream[*Entry[string, []byte]](entries), nil
}

type cachingIndexedMap struct {
	IndexedMap[string, []byte]
	entries cache.KeyValueCache[string, *Entry[string, []byte]]
	indexes cache.KeyValueCache[Index, *Entry[string, []byte]]
	mu      sync.RWMutex
	closeCh chan struct{}
}

func (m *cachingIndexedMap) open() error {
	ctx, cancel := context.WithCancel(context.Background())
	m.closeCh = make(chan struct{})
	go func() {
		<-m.closeCh
		cancel()
	}()

	events, err := m.IndexedMap.Events(ctx)
	if err != nil {
		cancel()
		return err
	}

	go func() {
		for {
			event, err := events.Next()
			if err == io.EOF || errors.IsCanceled(err) {
				return
			}
			if err != nil {
				log.Error(err)
			} else {
				switch e := event.(type) {
				case *Inserted[string, []byte]:
					m.update(e.Entry)
				case *Updated[string, []byte]:
					m.update(e.Entry)
				case *Removed[string, []byte]:
					m.remove(e.Entry)
				}
			}
		}
	}()
	return nil
}

func (m *cachingIndexedMap) update(entry *Entry[string, []byte]) {
	m.mu.RLock()
	stored, ok := m.indexes.Load(entry.Index)
	m.mu.RUnlock()
	if ok && entry.Version != 0 && stored.Version >= entry.Version {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries.Store(entry.Key, entry, func(stored *Entry[string, []byte]) bool {
		return entry.Version == 0 || entry.Version > stored.Version
	})
	m.indexes.Store(entry.Index, entry, func(stored *Entry[string, []byte]) bool {
		return entry.Version == 0 || entry.Version > stored.Version
	})
}

func (m *cachingIndexedMap) remove(entry *Entry[string, []byte]) {
	m.mu.RLock()
	stored, ok := m.indexes.Load(entry.Index)
	m.mu.RUnlock()
	if !ok || (entry.Version != 0 && stored.Version >= entry.Version) {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries.Delete(entry.Key, func(stored *Entry[string, []byte]) bool {
		return entry.Version == 0 || entry.Version >= stored.Version
	})
	m.indexes.Delete(entry.Index, func(stored *Entry[string, []byte]) bool {
		return entry.Version == 0 || entry.Version >= stored.Version
	})
}

func (m *cachingIndexedMap) Append(ctx context.Context, key string, value []byte, opts ...AppendOption) (*Entry[string, []byte], error) {
	entry, err := m.IndexedMap.Append(ctx, key, value, opts...)
	if err != nil {
		return nil, err
	}
	m.update(entry)
	return entry, nil
}

func (m *cachingIndexedMap) Update(ctx context.Context, key string, value []byte, opts ...UpdateOption) (*Entry[string, []byte], error) {
	entry, err := m.IndexedMap.Update(ctx, key, value, opts...)
	if err != nil {
		return nil, err
	}
	m.update(entry)
	return entry, nil
}

func (m *cachingIndexedMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry[string, []byte], error) {
	entry, err := m.IndexedMap.Remove(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	m.remove(entry)
	return entry, nil
}

func (m *cachingIndexedMap) RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry[string, []byte], error) {
	entry, err := m.IndexedMap.RemoveIndex(ctx, index, opts...)
	if err != nil {
		return nil, err
	}
	m.remove(entry)
	return entry, nil
}

func (m *cachingIndexedMap) Events(ctx context.Context, opts ...EventsOption) (EventStream[string, []byte], error) {
	events, err := m.IndexedMap.Events(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return stream.NewInterceptingStream[Event[string, []byte]](events, func(event Event[string, []byte]) {
		switch e := event.(type) {
		case *Inserted[string, []byte]:
			m.update(e.Entry)
		case *Updated[string, []byte]:
			m.update(e.Entry)
		case *Removed[string, []byte]:
			m.remove(e.Entry)
		}
	}), nil
}

func (m *cachingIndexedMap) Clear(ctx context.Context) error {
	m.entries.Purge()
	m.indexes.Purge()
	if err := m.IndexedMap.Clear(ctx); err != nil {
		return err
	}
	return nil
}

func (m *cachingIndexedMap) Close(ctx context.Context) error {
	defer close(m.closeCh)
	return m.IndexedMap.Close(ctx)
}
