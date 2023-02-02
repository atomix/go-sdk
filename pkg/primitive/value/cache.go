// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/util"
	"io"
	"sync"
)

func newCachingValue[V any](value Value[V]) (Value[V], error) {
	cm := &cachingValue[V]{
		Value: value,
	}
	if err := cm.open(); err != nil {
		return nil, err
	}
	return cm, nil
}

type cachingValue[V any] struct {
	Value[V]
	cache  *util.ValueCache[V]
	mu     sync.RWMutex
	cancel context.CancelFunc
}

func (v *cachingValue[V]) open() error {
	ctx, cancel := context.WithCancel(context.Background())
	v.cancel = cancel

	events, err := v.Value.Events(ctx)
	if err != nil {
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
				case *Created[V]:
					v.cache.Store(e.Value)
				case *Updated[V]:
					v.cache.Store(e.NewValue)
				case *Deleted[V]:
					v.cache.Delete(e.Value.Version)
				}
			}
		}
	}()
	return nil
}

func (v *cachingValue[V]) Set(ctx context.Context, value V, opts ...SetOption) (primitive.Versioned[V], error) {
	versioned, err := v.Value.Set(ctx, value, opts...)
	if err != nil {
		v.cache.Invalidate()
		return versioned, err
	}
	v.cache.Store(versioned)
	return versioned, err
}

func (v *cachingValue[V]) Update(ctx context.Context, value V, opts ...UpdateOption) (primitive.Versioned[V], error) {
	versioned, err := v.Value.Update(ctx, value, opts...)
	if err != nil {
		v.cache.Invalidate()
		return versioned, err
	}
	v.cache.Store(versioned)
	return versioned, err
}

func (v *cachingValue[V]) Get(ctx context.Context, opts ...GetOption) (primitive.Versioned[V], error) {
	if value, ok := v.cache.Load(); ok {
		return *value, nil
	}

	value, err := v.Value.Get(ctx, opts...)
	if err != nil {
		v.cache.Invalidate()
		return primitive.Versioned[V]{}, err
	}
	v.cache.Store(value)
	return value, err
}

func (v *cachingValue[V]) Delete(ctx context.Context, opts ...DeleteOption) error {
	err := v.Value.Delete(ctx, opts...)
	v.cache.Delete(0)
	if err != nil {
		return err
	}
	return nil
}

func (v *cachingValue[V]) Close(ctx context.Context) error {
	if err := v.Value.Close(ctx); err != nil {
		return err
	}
	if v.cancel != nil {
		v.cancel()
	}
	return nil
}
