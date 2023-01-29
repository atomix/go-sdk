// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/types/scalar"
	"io"
	"sync"
	"time"
)

func newMirroredMap[K scalar.Scalar, V any](m Map[K, V]) Map[K, V] {
	mm := &mirroredMap[K, V]{
		Map:     m,
		entries: make(map[K]primitive.Versioned[V]),
	}
	mm.open(context.Background())
	return mm
}

type mirroredMap[K scalar.Scalar, V any] struct {
	Map[K, V]
	entries map[K]primitive.Versioned[V]
	mu      sync.RWMutex
	cancel  context.CancelFunc
}

func (m *mirroredMap[K, V]) open(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	go m.watch(ctx)
}

func (m *mirroredMap[K, V]) watch(ctx context.Context) {
	events, err := m.Map.Events(ctx)
	if err != nil {
		time.AfterFunc(time.Second, func() {
			m.watch(ctx)
		})
	}
	for {
		event, err := events.Next()
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
			switch e := event.(type) {
			case *Inserted[K, V]:
				m.update(e.Entry.Key, e.Entry.Versioned)
			case *Updated[K, V]:
				m.update(e.NewEntry.Key, e.NewEntry.Versioned)
			case *Removed[K, V]:
				m.remove(e.Entry.Key, e.Entry.Version)
			}
		}
	}
}

func (m *mirroredMap[K, V]) update(key K, versioned primitive.Versioned[V]) {
	m.mu.RLock()
	value, ok := m.entries[key]
	m.mu.RUnlock()
	if ok && value.Version > versioned.Version {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok = m.entries[key]
	if ok && value.Version > versioned.Version {
		return
	}
	m.entries[key] = versioned
}

func (m *mirroredMap[K, V]) remove(key K, version primitive.Version) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.entries[key]
	if !ok || value.Version > version {
		return
	}
	delete(m.entries, key)
}

func (m *mirroredMap[K, V]) invalidate(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, key)
}

func (m *mirroredMap[K, V]) purge() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = make(map[K]primitive.Versioned[V])
}

func (m *mirroredMap[K, V]) Put(ctx context.Context, key K, value V, opts ...PutOption) (*Entry[K, V], error) {
	entry, err := m.Map.Put(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsAlreadyExists(err) && !errors.IsConflict(err) {
			m.invalidate(key)
		}
		return nil, err
	}
	m.update(key, entry.Versioned)
	return entry, nil
}

func (m *mirroredMap[K, V]) Insert(ctx context.Context, key K, value V, opts ...InsertOption) (*Entry[K, V], error) {
	entry, err := m.Map.Insert(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsAlreadyExists(err) && !errors.IsConflict(err) {
			m.invalidate(key)
		}
		return nil, err
	}
	m.update(key, entry.Versioned)
	return entry, nil
}

func (m *mirroredMap[K, V]) Update(ctx context.Context, key K, value V, opts ...UpdateOption) (*Entry[K, V], error) {
	entry, err := m.Map.Update(ctx, key, value, opts...)
	if err != nil {
		if !errors.IsConflict(err) {
			m.invalidate(key)
		}
		return nil, err
	}
	m.update(key, entry.Versioned)
	return entry, nil
}

func (m *mirroredMap[K, V]) Get(ctx context.Context, key K, opts ...GetOption) (*Entry[K, V], error) {
	m.mu.RLock()
	value, ok := m.entries[key]
	m.mu.RUnlock()
	if ok {
		return &Entry[K, V]{
			Key:       key,
			Versioned: value,
		}, nil
	}
	return m.Map.Get(ctx, key, opts...)
}

func (m *mirroredMap[K, V]) Remove(ctx context.Context, key K, opts ...RemoveOption) (*Entry[K, V], error) {
	entry, err := m.Map.Remove(ctx, key, opts...)
	if err != nil {
		if !errors.IsConflict(err) {
			m.invalidate(key)
		}
		return nil, err
	}
	m.remove(key, entry.Version)
	return entry, nil
}

func (m *mirroredMap[K, V]) Clear(ctx context.Context) error {
	if err := m.Map.Clear(ctx); err != nil {
		return err
	}
	m.purge()
	return nil
}

func (m *mirroredMap[K, V]) Close(ctx context.Context) error {
	if err := m.Map.Close(ctx); err != nil {
		return err
	}
	if m.cancel != nil {
		m.cancel()
	}
	return nil
}
