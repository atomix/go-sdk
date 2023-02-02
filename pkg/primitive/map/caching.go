// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/util"
	"io"
)

func newCachingMap(ctx context.Context, m Map[string, []byte], size int) (Map[string, []byte], error) {
	if size == 0 {
		mirror := util.NewKeyValueMirror[string, []byte]()
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

	cache, err := util.NewKeyValueLRU[string, []byte](size)
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
	if value, ok := m.cache.Load(key); ok {
		return &Entry[string, []byte]{
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

type mirroredMap struct {
	*cachingMap
	mirror *util.KeyValueMirror[string, []byte]
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
		m.cache.Store(entry.Key, entry.Versioned)
	}
	return m.cachingMap.open()
}

func (m *mirroredMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry[string, []byte], error) {
	if value, ok := m.cache.Load(key); ok {
		return &Entry[string, []byte]{
			Key:       key,
			Versioned: *value,
		}, nil
	}
	return nil, errors.NewNotFound("key %s not found", key)
}

func (m *mirroredMap) Entries(ctx context.Context) (EntryStream[string, []byte], error) {
	mirror := m.mirror.Copy()
	entries := make([]*Entry[string, []byte], 0, len(mirror))
	for key, value := range mirror {
		entries = append(entries, &Entry[string, []byte]{
			Key:       key,
			Versioned: value,
		})
	}
	return stream.NewSliceStream[*Entry[string, []byte]](entries), nil
}

type cachingMap struct {
	Map[string, []byte]
	cache   util.KeyValueCache[string, []byte]
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
					m.cache.Store(e.Entry.Key, e.Entry.Versioned)
				case *Updated[string, []byte]:
					m.cache.Store(e.NewEntry.Key, e.NewEntry.Versioned)
				case *Removed[string, []byte]:
					m.cache.Delete(e.Entry.Key, e.Entry.Version)
				}
			}
		}
	}()
	return nil
}

func (m *cachingMap) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry[string, []byte], error) {
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

func (m *cachingMap) Insert(ctx context.Context, key string, value []byte, opts ...InsertOption) (*Entry[string, []byte], error) {
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

func (m *cachingMap) Update(ctx context.Context, key string, value []byte, opts ...UpdateOption) (*Entry[string, []byte], error) {
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

func (m *cachingMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry[string, []byte], error) {
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

func (m *cachingMap) Clear(ctx context.Context) error {
	defer m.cache.Purge()
	if err := m.Map.Clear(ctx); err != nil {
		return err
	}
	return nil
}

func (m *cachingMap) Close(ctx context.Context) error {
	defer close(m.closeCh)
	return m.Map.Close(ctx)
}
