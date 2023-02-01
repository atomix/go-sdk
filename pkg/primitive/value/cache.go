// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/go-sdk/pkg/primitive"
	"sync"
)

func newCachingValue[V any](value Value[V]) Value[V] {
	return &cachingValue[V]{
		Value: value,
	}
}

type cachingValue[V any] struct {
	Value[V]
	value *primitive.Versioned[V]
	mu    sync.RWMutex
}

func (m *cachingValue[V]) Set(ctx context.Context, value V, opts ...SetOption) (primitive.Versioned[V], error) {
	versioned, err := m.Value.Set(ctx, value, opts...)
	if err != nil {
		m.mu.Lock()
		m.value = nil
		m.mu.Unlock()
		return versioned, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.value == nil || m.value.Version < versioned.Version {
		m.value = &versioned
	}
	return versioned, err
}

func (m *cachingValue[V]) Update(ctx context.Context, value V, opts ...UpdateOption) (primitive.Versioned[V], error) {
	versioned, err := m.Value.Update(ctx, value, opts...)
	if err != nil {
		m.mu.Lock()
		m.value = nil
		m.mu.Unlock()
		return versioned, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.value == nil || m.value.Version < versioned.Version {
		m.value = &versioned
	}
	return versioned, err
}

func (m *cachingValue[V]) Get(ctx context.Context, opts ...GetOption) (primitive.Versioned[V], error) {
	m.mu.RLock()
	value := m.value
	m.mu.RUnlock()
	if value != nil {
		return *value, nil
	}

	versioned, err := m.Value.Get(ctx, opts...)
	if err != nil {
		m.mu.Lock()
		m.value = nil
		m.mu.Unlock()
		return versioned, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.value == nil || m.value.Version < versioned.Version {
		m.value = &versioned
	}
	return versioned, err
}

func (m *cachingValue[V]) Delete(ctx context.Context, opts ...DeleteOption) error {
	err := m.Value.Delete(ctx, opts...)
	m.mu.Lock()
	m.value = nil
	m.mu.Unlock()
	if err != nil {
		return err
	}
	return nil
}
