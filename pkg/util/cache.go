// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"sync"
)

func NewValueCache[V any]() *ValueCache[V] {
	return &ValueCache[V]{}
}

type ValueCache[V any] struct {
	value *primitive.Versioned[V]
	mu    sync.RWMutex
}

func (c *ValueCache[T]) Store(value primitive.Versioned[T]) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.value == nil || value.Version == 0 || value.Version > c.value.Version {
		c.value = &value
		return true
	}
	return false
}

func (c *ValueCache[T]) Load() (*primitive.Versioned[T], bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.value != nil {
		return c.value, true
	}
	return nil, false
}

func (c *ValueCache[T]) Delete(version primitive.Version) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.value != nil && (version == 0 || version >= c.value.Version) {
		c.value = nil
		return true
	}
	return false
}

func (c *ValueCache[V]) Invalidate() {
	c.mu.Lock()
	c.value = nil
	c.mu.Unlock()
}

type KeyValueCache[K comparable, V any] interface {
	Store(key K, value primitive.Versioned[V]) bool
	Load(key K) (*primitive.Versioned[V], bool)
	Delete(key K, version primitive.Version) bool
	Invalidate(key K) bool
	Purge()
}

func NewKeyValueLRU[K comparable, V any](size int) (*KeyValueLRU[K, V], error) {
	cache, err := simplelru.NewLRU[K, primitive.Versioned[V]](size, nil)
	if err != nil {
		return nil, err
	}
	return &KeyValueLRU[K, V]{
		cache: cache,
	}, nil
}

type KeyValueLRU[K comparable, V any] struct {
	cache *simplelru.LRU[K, primitive.Versioned[V]]
	mu    sync.RWMutex
}

func (c *KeyValueLRU[K, V]) Store(key K, value primitive.Versioned[V]) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cached, ok := c.cache.Peek(key); !ok || value.Version == 0 || value.Version > cached.Version {
		c.cache.Add(key, value)
		return true
	}
	return false
}

func (c *KeyValueLRU[K, V]) Load(key K) (*primitive.Versioned[V], bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if value, ok := c.cache.Get(key); ok {
		return &value, true
	}
	return nil, false
}

func (c *KeyValueLRU[K, V]) Delete(key K, version primitive.Version) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cached, ok := c.cache.Peek(key); ok && (version == 0 || version >= cached.Version) {
		return c.cache.Remove(key)
	}
	return false
}

func (c *KeyValueLRU[K, V]) Invalidate(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cache.Remove(key)
}

func (c *KeyValueLRU[K, V]) Purge() {
	c.mu.Lock()
	c.cache.Purge()
	c.mu.Unlock()
}

func NewKeyValueMirror[K comparable, V any]() *KeyValueMirror[K, V] {
	return &KeyValueMirror[K, V]{
		values: make(map[K]primitive.Versioned[V]),
	}
}

type KeyValueMirror[K comparable, V any] struct {
	values map[K]primitive.Versioned[V]
	mu     sync.RWMutex
}

func (c *KeyValueMirror[K, V]) Store(key K, value primitive.Versioned[V]) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cached, ok := c.values[key]; !ok || value.Version == 0 || value.Version > cached.Version {
		c.values[key] = value
		return true
	}
	return false
}

func (c *KeyValueMirror[K, V]) Load(key K) (*primitive.Versioned[V], bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if value, ok := c.values[key]; ok {
		return &value, true
	}
	return nil, false
}

func (c *KeyValueMirror[K, V]) Delete(key K, version primitive.Version) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cached, ok := c.values[key]; ok && (version == 0 || version >= cached.Version) {
		delete(c.values, key)
		return true
	}
	return false
}

func (c *KeyValueMirror[K, V]) Invalidate(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.values[key]; ok {
		delete(c.values, key)
		return true
	}
	return false
}

func (c *KeyValueMirror[K, V]) Purge() {
	c.mu.Lock()
	c.values = make(map[K]primitive.Versioned[V])
	c.mu.Unlock()
}

func (c *KeyValueMirror[K, V]) Copy() map[K]primitive.Versioned[V] {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entries := make(map[K]primitive.Versioned[V])
	for key, value := range c.values {
		entries[key] = value
	}
	return entries
}
