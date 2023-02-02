// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/types/scalar"
	"github.com/atomix/go-sdk/pkg/util"
	"io"
	"sync"
)

func newCachingMap[K scalar.Scalar, V any](m Map[K, V], size int) (Map[K, V], error) {
	cache, err := util.NewKeyValueCache[K, V](size)
	if err != nil {
		return nil, err
	}
	cm := &cachingMap[K, V]{
		Map:   m,
		cache: cache,
	}
	cm.open()
	return cm, nil
}

type cachingMap[K scalar.Scalar, V any] struct {
	Map[K, V]
	cache  util.KeyValueCache[K, V]
	mu     sync.RWMutex
	cancel context.CancelFunc
}

func (m *cachingMap[K, V]) open() error {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	events, err := m.Map.Events(ctx)
	if err != nil {
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
				case *Inserted[K, V]:
					m.cache.Store(e.Entry.Key, e.Entry.Versioned)
				case *Updated[K, V]:
					m.cache.Store(e.NewEntry.Key, e.NewEntry.Versioned)
				case *Removed[K, V]:
					m.cache.Delete(e.Entry.Key, e.Entry.Version)
				}
			}
		}
	}()
	return nil
}

func (m *cachingMap[K, V]) Put(ctx context.Context, key K, value V, opts ...PutOption) (*Entry[K, V], error) {
	entry, err := m.Map.Put(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsAlreadyExists(err) && !errors.IsConflict(err) {
			m.cache.Invalidate(key)
		}
		return nil, err
	}
	m.cache.Store(key, entry.Versioned)
	return entry, nil
}

func (m *cachingMap[K, V]) Insert(ctx context.Context, key K, value V, opts ...InsertOption) (*Entry[K, V], error) {
	entry, err := m.Map.Insert(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsAlreadyExists(err) && !errors.IsConflict(err) {
			m.cache.Invalidate(key)
		}
		return nil, err
	}
	m.cache.Store(key, entry.Versioned)
	return entry, nil
}

func (m *cachingMap[K, V]) Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error) {
	entry, err := m.Map.Update(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsConflict(err) {
			m.cache.Invalidate(key)
		}
		return nil, err
	}
	m.cache.Store(key, entry.Versioned)
	return entry, nil
}

func (m *cachingMap[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	if value, ok := m.cache.Load(key); ok {
		return &Entry[K, V]{
			Key:       key,
			Versioned: *value,
		}, nil
	}

	entry, err := m.Map.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	m.cache.Store(entry.Key, entry.Versioned)
	return entry, nil
}

func (m *cachingMap[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	entry, err := m.Map.Remove(ctx, key, opts...)
	if err != nil {
		if !errors.IsConflict(err) {
			m.cache.Invalidate(key)
		}
		return nil, err
	}
	m.cache.Delete(key, entry.Version)
	return entry, nil
}

func (m *cachingMap[K, V]) Clear(ctx context.Context) error {
	defer m.cache.Purge()
	if err := m.Map.Clear(ctx); err != nil {
		return err
	}
	return nil
}

func (m *cachingMap[K, V]) Close(ctx context.Context) error {
	if err := m.Map.Close(ctx); err != nil {
		return err
	}
	if m.cancel != nil {
		m.cancel()
	}
	return nil
}
