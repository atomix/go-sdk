// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/util/cache"
	"io"
)

func newCachingSet(ctx context.Context, m Set[string], size int) (Set[string], error) {
	var cm *cachingSet
	if size == 0 {
		cm = &cachingSet{
			Set:   m,
			cache: cache.NewKeyValueMirror[string, bool](),
		}
		elements, err := m.Elements(ctx)
		if err != nil {
			return nil, err
		}
		for {
			element, err := elements.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			cm.cache.Store(element, true, func(value bool) bool {
				return true
			})
		}
	} else {
		cache, err := cache.NewKeyValueLRU[string, bool](size)
		if err != nil {
			return nil, err
		}
		cm = &cachingSet{
			Set:   m,
			cache: cache,
		}
	}

	if err := cm.open(); err != nil {
		return nil, err
	}
	return cm, nil
}

type cachingSet struct {
	Set[string]
	cache   cache.KeyValueCache[string, bool]
	closeCh chan struct{}
}

func (m *cachingSet) open() error {
	ctx, cancel := context.WithCancel(context.Background())
	m.closeCh = make(chan struct{})
	go func() {
		<-m.closeCh
		cancel()
	}()

	events, err := m.Set.Events(ctx)
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
				case *Added[string]:
					m.cache.Store(e.Element, true, func(value bool) bool {
						return true
					})
				case *Removed[string]:
					m.cache.Delete(e.Element, func(value bool) bool {
						return true
					})
				}
			}
		}
	}()
	return nil
}

func (m *cachingSet) Add(ctx context.Context, value string) (bool, error) {
	if ok, err := m.Set.Add(ctx, value); err != nil {
		return false, err
	} else if ok {
		m.cache.Store(value, true, func(value bool) bool {
			return true
		})
		return true, nil
	}
	return false, nil
}

func (m *cachingSet) Contains(ctx context.Context, value string) (bool, error) {
	if exists, ok := m.cache.Load(value); ok {
		return exists, nil
	}

	if ok, err := m.Set.Contains(ctx, value); err != nil {
		return false, err
	} else if ok {
		m.cache.Store(value, true, func(value bool) bool {
			return true
		})
		return true, nil
	}
	return false, nil
}

func (m *cachingSet) Remove(ctx context.Context, value string) (bool, error) {
	if ok, err := m.Set.Remove(ctx, value); err != nil {
		return false, err
	} else if ok {
		m.cache.Delete(value, func(value bool) bool {
			return true
		})
		return true, nil
	}
	return false, nil
}

func (m *cachingSet) Clear(ctx context.Context) error {
	defer m.cache.Purge()
	if err := m.Set.Clear(ctx); err != nil {
		return err
	}
	return nil
}

func (m *cachingSet) Close(ctx context.Context) error {
	defer close(m.closeCh)
	return m.Set.Close(ctx)
}
