// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/types/scalar"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"io"
	"sync"
	"time"
)

func newCachingMap[K scalar.Scalar, V any](m Map[K, V], size int) (Map[K, V], error) {
	cache, err := simplelru.NewLRU[K, primitive.Versioned[V]](size, nil)
	if err != nil {
		return nil, err
	}
	cm := &cachingMap[K, V]{
		Map:   m,
		cache: cache,
	}
	cm.open(context.Background())
	return cm, nil
}

type cachingMap[K scalar.Scalar, V any] struct {
	Map[K, V]
	cache  *simplelru.LRU[K, primitive.Versioned[V]]
	mu     sync.RWMutex
	cancel context.CancelFunc
}

func (m *cachingMap[K, V]) open(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	go m.watch(ctx)
}

func (m *cachingMap[K, V]) watch(ctx context.Context) {
	entries, err := m.Map.Watch(ctx)
	if err != nil {
		time.AfterFunc(time.Second, func() {
			m.watch(ctx)
		})
	}
	for {
		entry, err := entries.Next()
		if errors.IsCanceled(err) {
			return
		}
		if err == io.EOF {
			time.AfterFunc(time.Second, func() {
				m.watch(ctx)
			})
		}
		if err != nil {
			log.Error(err)
		} else {
			m.update(entry.Key, entry.Versioned)
		}
	}
}

func (m *cachingMap[K, V]) update(key K, versioned primitive.Versioned[V]) {
	m.mu.RLock()
	cached, ok := m.cache.Peek(key)
	m.mu.RUnlock()
	if ok && cached.Version > versioned.Version {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	cached, ok = m.cache.Peek(key)
	if ok && cached.Version > versioned.Version {
		return
	}
	m.cache.Add(key, versioned)
}

func (m *cachingMap[K, V]) invalidate(key K, version primitive.Version) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cached, ok := m.cache.Peek(key)
	if !ok || cached.Version > version {
		return
	}
	m.cache.Remove(key)
}

func (m *cachingMap[K, V]) purge() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache.Purge()
}

func (m *cachingMap[K, V]) Put(ctx context.Context, key K, value V, opts ...PutOption) (*Entry[K, V], error) {
	entry, err := m.Map.Put(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsAlreadyExists(err) && !errors.IsConflict(err) {
			m.invalidate(key, entry.Version)
		}
		return nil, err
	}
	m.update(key, entry.Versioned)
	return entry, nil
}

func (m *cachingMap[K, V]) Insert(ctx context.Context, key K, value V, opts ...InsertOption) (*Entry[K, V], error) {
	entry, err := m.Map.Insert(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsAlreadyExists(err) && !errors.IsConflict(err) {
			m.invalidate(key, entry.Version)
		}
		return nil, err
	}
	m.update(key, entry.Versioned)
	return entry, nil
}

func (m *cachingMap[K, V]) Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error) {
	entry, err := m.Map.Update(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsConflict(err) {
			m.invalidate(key, entry.Version)
		}
		return nil, err
	}
	m.update(key, entry.Versioned)
	return entry, nil
}

func (m *cachingMap[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	m.mu.Lock()
	cached, ok := m.cache.Get(key)
	m.mu.Unlock()
	if ok {
		return &Entry[K, V]{
			Key:       key,
			Versioned: cached,
		}, nil
	}
	return m.Map.Get(ctx, key, opts...)
}

func (m *cachingMap[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	entry, err := m.Map.Remove(ctx, key, opts...)
	if err != nil {
		if !errors.IsConflict(err) {
			m.invalidate(key, entry.Version)
		}
		return nil, err
	}
	m.invalidate(key, entry.Version)
	return entry, nil
}

func (m *cachingMap[K, V]) Clear(ctx context.Context) error {
	if err := m.Map.Clear(ctx); err != nil {
		return err
	}
	m.purge()
	return nil
}
