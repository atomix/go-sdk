// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/util"
	"io"
)

func newCachingSet(ctx context.Context, m Set[string], size int) (Set[string], error) {
	var cm *cachingSet
	if size == 0 {
		cm = &cachingSet{
			Set:   m,
			cache: util.NewKeyValueMirror[string, bool](),
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
			cm.cache.Store(element, primitive.Versioned[bool]{Value: true})
		}
	} else {
		cache, err := util.NewKeyValueLRU[string, bool](size)
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
	cache   util.KeyValueCache[string, bool]
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
					m.cache.Store(e.Element, primitive.Versioned[bool]{Value: true})
				case *Removed[string]:
					m.cache.Invalidate(e.Element)
				}
			}
		}
	}()
	return nil
}

func (m *cachingSet) Add(ctx context.Context, value string) (bool, error) {
	if ok, err := m.Set.Add(ctx, value); err != nil {
		if !errors.IsAlreadyExists(err) && !errors.IsConflict(err) {
			m.cache.Invalidate(value)
		}
		return false, err
	} else if ok {
		m.cache.Store(value, primitive.Versioned[bool]{Value: true})
		return true, nil
	}
	return false, nil
}

func (m *cachingSet) Contains(ctx context.Context, value string) (bool, error) {
	if exists, ok := m.cache.Load(value); ok {
		return exists.Value, nil
	}

	if ok, err := m.Set.Contains(ctx, value); err != nil {
		return false, err
	} else if ok {
		m.cache.Store(value, primitive.Versioned[bool]{Value: true})
		return true, nil
	}
	return false, nil
}

func (m *cachingSet) Remove(ctx context.Context, value string) (bool, error) {
	if ok, err := m.Set.Remove(ctx, value); err != nil {
		if !errors.IsConflict(err) {
			m.cache.Invalidate(value)
		}
		return false, err
	} else if ok {
		m.cache.Invalidate(value)
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
