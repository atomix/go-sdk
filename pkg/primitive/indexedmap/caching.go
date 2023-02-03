// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/util"
	"io"
)

func newCachingIndexedMap(ctx context.Context, m IndexedMap[string, []byte], size int) (IndexedMap[string, []byte], error) {
	if size == 0 {
		entries := util.NewKeyValueMirror[string, *Entry[string, []byte]]()
		indexes := util.NewKeyValueMirror[Index, *Entry[string, []byte]]()
		mm := &mirroredIndexedMap{
			cachingIndexedMap: &cachingIndexedMap{
				IndexedMap: m,
				entries:    entries,
			},
			entries: entries,
			indexes: indexes,
		}
		if err := mm.open(ctx); err != nil {
			return nil, err
		}
		return mm, nil
	}

	entries, err := util.NewKeyValueLRU[string, *Entry[string, []byte]](size)
	if err != nil {
		return nil, err
	}
	indexes, err := util.NewKeyValueLRU[Index, *Entry[string, []byte]](size)
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
	if value, ok := m.entries.Load(key); ok {
		return &Entry[string, []byte]{
			Key:       key,
			Versioned: value.Value.Versioned,
		}, nil
	}

	entry, err := m.IndexedMap.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	m.entries.Store(entry.Key, entry, entry.Version)
	m.indexes.Store(entry.Index, entry, entry.Version)
	return entry, nil
}

type mirroredIndexedMap struct {
	*cachingIndexedMap
	entries *util.KeyValueMirror[string, *Entry[string, []byte]]
	indexes *util.KeyValueMirror[Index, *Entry[string, []byte]]
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
		m.entries.Store(entry.Key, entry, entry.Version)
		m.indexes.Store(entry.Index, entry, entry.Version)
	}
	return m.cachingIndexedMap.open()
}

func (m *mirroredIndexedMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry[string, []byte], error) {
	if value, ok := m.entries.Load(key); ok {
		return value.Value, nil
	}
	return nil, errors.NewNotFound("key %s not found", key)
}

func (m *mirroredIndexedMap) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry[string, []byte], error) {
	if value, ok := m.indexes.Load(index); ok {
		return value.Value, nil
	}
	return nil, errors.NewNotFound("index %d not found", index)
}

func (m *mirroredIndexedMap) Entries(ctx context.Context) (EntryStream[string, []byte], error) {
	mirror := m.entries.Copy()
	entries := make([]*Entry[string, []byte], 0, len(mirror))
	for _, value := range mirror {
		entries = append(entries, value.Value)
	}
	return stream.NewSliceStream[*Entry[string, []byte]](entries), nil
}

type cachingIndexedMap struct {
	IndexedMap[string, []byte]
	entries util.KeyValueCache[string, *Entry[string, []byte]]
	indexes util.KeyValueCache[Index, *Entry[string, []byte]]
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
					m.entries.Store(e.Entry.Key, e.Entry, e.Entry.Version)
					m.indexes.Store(e.Entry.Index, e.Entry, e.Entry.Version)
				case *Updated[string, []byte]:
					m.entries.Store(e.Entry.Key, e.Entry, e.Entry.Version)
					m.indexes.Store(e.Entry.Index, e.Entry, e.Entry.Version)
				case *Removed[string, []byte]:
					m.entries.Delete(e.Entry.Key, e.Entry.Version)
					m.indexes.Delete(e.Entry.Index, e.Entry.Version)
				}
			}
		}
	}()
	return nil
}

func (m *cachingIndexedMap) Append(ctx context.Context, key string, value []byte, opts ...AppendOption) (*Entry[string, []byte], error) {
	entry, err := m.IndexedMap.Append(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsAlreadyExists(err) && !errors.IsConflict(err) {
			m.entries.Invalidate(key)
		}
		return nil, err
	}
	m.entries.Store(key, entry, entry.Version)
	m.indexes.Store(entry.Index, entry, entry.Version)
	return entry, nil
}

func (m *cachingIndexedMap) Update(ctx context.Context, key string, value []byte, opts ...UpdateOption) (*Entry[string, []byte], error) {
	entry, err := m.IndexedMap.Update(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsConflict(err) {
			m.entries.Invalidate(key)
		}
		return nil, err
	}
	m.entries.Store(key, entry, entry.Version)
	m.indexes.Store(entry.Index, entry, entry.Version)
	return entry, nil
}

func (m *cachingIndexedMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry[string, []byte], error) {
	entry, err := m.IndexedMap.Remove(ctx, key, opts...)
	if err != nil {
		if !errors.IsConflict(err) {
			m.entries.Invalidate(key)
		}
		return nil, err
	}
	m.entries.Delete(key, entry.Version)
	m.indexes.Delete(entry.Index, entry.Version)
	return entry, nil
}

func (m *cachingIndexedMap) RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry[string, []byte], error) {
	entry, err := m.IndexedMap.RemoveIndex(ctx, index, opts...)
	if err != nil {
		if !errors.IsConflict(err) {
			m.indexes.Invalidate(index)
		}
		return nil, err
	}
	m.entries.Delete(entry.Key, entry.Version)
	m.indexes.Delete(entry.Index, entry.Version)
	return entry, nil
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
