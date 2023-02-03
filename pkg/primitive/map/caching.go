// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/util/cache"
	"io"
)

func newCachingMap(ctx context.Context, m Map[string, []byte], size int) (Map[string, []byte], error) {
	if size == 0 {
		mirror := cache.NewKeyValueMirror[string, *Entry[string, []byte]]()
		mm := &mirroredMap{
			cachingMap: &cachingMap{
				Map:   m,
				cache: mirror,
			},
			mirror: mirror,
		}
		if err := mm.open(ctx); err != nil {
			return nil, err
		}
		return mm, nil
	}

	cache, err := cache.NewKeyValueLRU[string, *Entry[string, []byte]](size)
	if err != nil {
		return nil, err
	}
	cm := &cachedMap{
		cachingMap: &cachingMap{
			Map:   m,
			cache: cache,
		},
	}
	if err := cm.open(); err != nil {
		return nil, err
	}
	return cm, nil
}

type cachedMap struct {
	*cachingMap
}

func (m *cachedMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry[string, []byte], error) {
	if entry, ok := m.cache.Load(key); ok {
		return entry, nil
	}

	entry, err := m.Map.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	m.cache.Store(entry.Key, entry, func(stored *Entry[string, []byte]) bool {
		return entry.Version == 0 || entry.Version > stored.Version
	})
	return entry, nil
}

type mirroredMap struct {
	*cachingMap
	mirror *cache.KeyValueMirror[string, *Entry[string, []byte]]
}

func (m *mirroredMap) open(ctx context.Context) error {
	entries, err := m.Map.List(ctx)
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
		m.cache.Store(entry.Key, entry, func(stored *Entry[string, []byte]) bool {
			return entry.Version == 0 || entry.Version > stored.Version
		})
	}
	return m.cachingMap.open()
}

func (m *mirroredMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry[string, []byte], error) {
	if entry, ok := m.cache.Load(key); ok {
		return entry, nil
	}
	return nil, errors.NewNotFound("key %s not found", key)
}

func (m *mirroredMap) Entries(ctx context.Context) (EntryStream[string, []byte], error) {
	mirror := m.mirror.Copy()
	entries := make([]*Entry[string, []byte], 0, len(mirror))
	for _, entry := range mirror {
		entries = append(entries, entry)
	}
	return stream.NewSliceStream[*Entry[string, []byte]](entries), nil
}

type cachingMap struct {
	Map[string, []byte]
	cache   cache.KeyValueCache[string, *Entry[string, []byte]]
	closeCh chan struct{}
}

func (m *cachingMap) open() error {
	ctx, cancel := context.WithCancel(context.Background())
	m.closeCh = make(chan struct{})
	go func() {
		<-m.closeCh
		cancel()
	}()

	events, err := m.Map.Events(ctx)
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
					m.cache.Store(e.Entry.Key, e.Entry, func(stored *Entry[string, []byte]) bool {
						return e.Entry.Version == 0 || e.Entry.Version > stored.Version
					})
				case *Updated[string, []byte]:
					m.cache.Store(e.Entry.Key, e.Entry, func(stored *Entry[string, []byte]) bool {
						return e.Entry.Version == 0 || e.Entry.Version > stored.Version
					})
				case *Removed[string, []byte]:
					m.cache.Delete(e.Entry.Key, func(stored *Entry[string, []byte]) bool {
						return e.Entry.Version == 0 || e.Entry.Version >= stored.Version
					})
				}
			}
		}
	}()
	return nil
}

func (m *cachingMap) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry[string, []byte], error) {
	entry, err := m.Map.Put(ctx, key, value, opts...)
	if err != nil {
		return nil, err
	}
	m.cache.Store(key, entry, func(stored *Entry[string, []byte]) bool {
		return entry.Version == 0 || entry.Version > stored.Version
	})
	return entry, nil
}

func (m *cachingMap) Insert(ctx context.Context, key string, value []byte, opts ...InsertOption) (*Entry[string, []byte], error) {
	entry, err := m.Map.Insert(ctx, key, value, opts...)
	if err != nil {
		return nil, err
	}
	m.cache.Store(key, entry, func(stored *Entry[string, []byte]) bool {
		return entry.Version == 0 || entry.Version > stored.Version
	})
	return entry, nil
}

func (m *cachingMap) Update(ctx context.Context, key string, value []byte, opts ...UpdateOption) (*Entry[string, []byte], error) {
	entry, err := m.Map.Update(ctx, key, value, opts...)
	if err != nil {
		return nil, err
	}
	m.cache.Store(key, entry, func(stored *Entry[string, []byte]) bool {
		return entry.Version == 0 || entry.Version > stored.Version
	})
	return entry, nil
}

func (m *cachingMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry[string, []byte], error) {
	entry, err := m.Map.Remove(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	m.cache.Delete(key, func(stored *Entry[string, []byte]) bool {
		return entry.Version == 0 || entry.Version >= stored.Version
	})
	return entry, nil
}

func (m *cachingMap) Events(ctx context.Context, opts ...EventsOption) (EventStream[string, []byte], error) {
	events, err := m.Map.Events(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return stream.NewInterceptingStream[Event[string, []byte]](events, func(event Event[string, []byte]) {
		switch e := event.(type) {
		case *Inserted[string, []byte]:
			m.cache.Store(e.Entry.Key, e.Entry, func(stored *Entry[string, []byte]) bool {
				return e.Entry.Version == 0 || e.Entry.Version > stored.Version
			})
		case *Updated[string, []byte]:
			m.cache.Store(e.Entry.Key, e.Entry, func(stored *Entry[string, []byte]) bool {
				return e.Entry.Version == 0 || e.Entry.Version > stored.Version
			})
		case *Removed[string, []byte]:
			m.cache.Delete(e.Entry.Key, func(stored *Entry[string, []byte]) bool {
				return e.Entry.Version == 0 || e.Entry.Version >= stored.Version
			})
		}
	}), nil
}

func (m *cachingMap) Clear(ctx context.Context) error {
	m.cache.Purge()
	if err := m.Map.Clear(ctx); err != nil {
		return err
	}
	return nil
}

func (m *cachingMap) Close(ctx context.Context) error {
	defer close(m.closeCh)
	return m.Map.Close(ctx)
}
