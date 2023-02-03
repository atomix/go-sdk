// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"github.com/hashicorp/golang-lru/v2/simplelru"
)

func NewUnsafeValueCache[V any]() *UnsafeValueCache[V] {
	return &UnsafeValueCache[V]{}
}

type UnsafeValueCache[V any] struct {
	value *V
}

func (c *UnsafeValueCache[V]) Store(value V, predicate Predicate[V]) bool {
	if c.value == nil || predicate(*c.value) {
		c.value = &value
		return true
	}
	return false
}

func (c *UnsafeValueCache[V]) Load() (V, bool) {
	var value V
	if c.value != nil {
		return *c.value, true
	}
	return value, false
}

func (c *UnsafeValueCache[V]) Delete(predicate Predicate[V]) bool {
	if c.value != nil && predicate(*c.value) {
		c.value = nil
		return true
	}
	return false
}

func NewUnsafeKeyValueLRU[K comparable, V any](size int) (*UnsafeKeyValueLRU[K, V], error) {
	cache, err := simplelru.NewLRU[K, V](size, nil)
	if err != nil {
		return nil, err
	}
	return &UnsafeKeyValueLRU[K, V]{
		cache: cache,
	}, nil
}

type UnsafeKeyValueLRU[K comparable, V any] struct {
	cache *simplelru.LRU[K, V]
}

func (c *UnsafeKeyValueLRU[K, V]) Store(key K, value V, predicate Predicate[V]) bool {
	if cached, ok := c.cache.Peek(key); !ok || predicate(cached) {
		return c.cache.Add(key, value)
	}
	return false
}

func (c *UnsafeKeyValueLRU[K, V]) Load(key K) (V, bool) {
	var v V
	if value, ok := c.cache.Get(key); ok {
		return value, true
	}
	return v, false
}

func (c *UnsafeKeyValueLRU[K, V]) Delete(key K, predicate Predicate[V]) bool {
	if cached, ok := c.cache.Peek(key); ok && predicate(cached) {
		return c.cache.Remove(key)
	}
	return false
}

func (c *UnsafeKeyValueLRU[K, V]) Purge() {
	c.cache.Purge()
}

func NewUnsafeKeyValueMirror[K comparable, V any]() *UnsafeKeyValueMirror[K, V] {
	return &UnsafeKeyValueMirror[K, V]{
		values: make(map[K]V),
	}
}

type UnsafeKeyValueMirror[K comparable, V any] struct {
	values map[K]V
}

func (c *UnsafeKeyValueMirror[K, V]) Store(key K, value V, predicate Predicate[V]) bool {
	if cached, ok := c.values[key]; !ok || predicate(cached) {
		c.values[key] = value
		return true
	}
	return false
}

func (c *UnsafeKeyValueMirror[K, V]) Load(key K) (V, bool) {
	var value V
	if value, ok := c.values[key]; ok {
		return value, true
	}
	return value, false
}

func (c *UnsafeKeyValueMirror[K, V]) Delete(key K, predicate Predicate[V]) bool {
	if cached, ok := c.values[key]; ok && predicate(cached) {
		delete(c.values, key)
		return true
	}
	return false
}

func (c *UnsafeKeyValueMirror[K, V]) Invalidate(key K) bool {
	if _, ok := c.values[key]; ok {
		delete(c.values, key)
		return true
	}
	return false
}

func (c *UnsafeKeyValueMirror[K, V]) Purge() {
	c.values = make(map[K]V)
}

func (c *UnsafeKeyValueMirror[K, V]) Copy() map[K]V {
	entries := make(map[K]V)
	for key, value := range c.values {
		entries[key] = value
	}
	return entries
}
