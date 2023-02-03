// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"sync"
)

func NewValueCache[V any]() *ValueCache[V] {
	return &ValueCache[V]{
		UnsafeValueCache: NewUnsafeValueCache[V](),
	}
}

type ValueCache[V any] struct {
	*UnsafeValueCache[V]
	mu sync.RWMutex
}

func (c *ValueCache[V]) Store(value V, predicate Predicate[V]) bool {
	c.mu.RLock()
	cached, ok := c.UnsafeValueCache.Load()
	c.mu.RUnlock()
	if ok && !predicate(cached) {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.UnsafeValueCache.Store(value, predicate)
}

func (c *ValueCache[V]) Load() (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.UnsafeValueCache.Load()
}

func (c *ValueCache[V]) Delete(predicate Predicate[V]) bool {
	c.mu.RLock()
	cached, ok := c.UnsafeValueCache.Load()
	c.mu.RUnlock()
	if !ok || !predicate(cached) {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.UnsafeValueCache.Delete(predicate)
}

type Predicate[T any] func(T) bool

type KeyValueCache[K comparable, V any] interface {
	Store(key K, value V, predicate Predicate[V]) bool
	Load(key K) (V, bool)
	Delete(key K, predicate Predicate[V]) bool
	Purge()
}

func NewKeyValueLRU[K comparable, V any](size int) (*KeyValueLRU[K, V], error) {
	cache, err := NewUnsafeKeyValueLRU[K, V](size)
	if err != nil {
		return nil, err
	}
	return &KeyValueLRU[K, V]{
		UnsafeKeyValueLRU: cache,
	}, nil
}

type KeyValueLRU[K comparable, V any] struct {
	*UnsafeKeyValueLRU[K, V]
	mu sync.RWMutex
}

func (c *KeyValueLRU[K, V]) Store(key K, value V, predicate Predicate[V]) bool {
	c.mu.RLock()
	cached, ok := c.UnsafeKeyValueLRU.Load(key)
	c.mu.RUnlock()
	if ok && !predicate(cached) {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.UnsafeKeyValueLRU.Store(key, value, predicate)
}

func (c *KeyValueLRU[K, V]) Load(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.UnsafeKeyValueLRU.Load(key)
}

func (c *KeyValueLRU[K, V]) Delete(key K, predicate Predicate[V]) bool {
	c.mu.RLock()
	cached, ok := c.UnsafeKeyValueLRU.Load(key)
	c.mu.RUnlock()
	if !ok || !predicate(cached) {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.UnsafeKeyValueLRU.Delete(key, predicate)
}

func (c *KeyValueLRU[K, V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.UnsafeKeyValueLRU.Purge()
}

func NewKeyValueMirror[K comparable, V any]() *KeyValueMirror[K, V] {
	return &KeyValueMirror[K, V]{
		UnsafeKeyValueMirror: NewUnsafeKeyValueMirror[K, V](),
	}
}

type KeyValueMirror[K comparable, V any] struct {
	*UnsafeKeyValueMirror[K, V]
	mu sync.RWMutex
}

func (c *KeyValueMirror[K, V]) Store(key K, value V, predicate Predicate[V]) bool {
	c.mu.RLock()
	cached, ok := c.UnsafeKeyValueMirror.Load(key)
	c.mu.RUnlock()
	if ok && !predicate(cached) {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.UnsafeKeyValueMirror.Store(key, value, predicate)
}

func (c *KeyValueMirror[K, V]) Load(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.UnsafeKeyValueMirror.Load(key)
}

func (c *KeyValueMirror[K, V]) Delete(key K, predicate Predicate[V]) bool {
	c.mu.RLock()
	cached, ok := c.UnsafeKeyValueMirror.Load(key)
	c.mu.RUnlock()
	if !ok || !predicate(cached) {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.UnsafeKeyValueMirror.Delete(key, predicate)
}

func (c *KeyValueMirror[K, V]) Invalidate(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.UnsafeKeyValueMirror.Invalidate(key)
}

func (c *KeyValueMirror[K, V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.UnsafeKeyValueMirror.Purge()
}

func (c *KeyValueMirror[K, V]) Copy() map[K]V {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.UnsafeKeyValueMirror.Copy()
}
