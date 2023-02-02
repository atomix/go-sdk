// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/primitive"
	"io"
	"sync"
	"time"
)

func newCachingValue[V any](value Value[V]) Value[V] {
	cm := &cachingValue[V]{
		Value: value,
	}
	cm.open(context.Background())
	return cm
}

type cachingValue[V any] struct {
	Value[V]
	value  *primitive.Versioned[V]
	mu     sync.RWMutex
	cancel context.CancelFunc
}

func (v *cachingValue[V]) open(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	v.cancel = cancel
	go v.watch(ctx)
}

func (v *cachingValue[V]) watch(ctx context.Context) {
	events, err := v.Value.Events(ctx)
	if err != nil {
		time.AfterFunc(time.Second, func() {
			v.watch(ctx)
		})
	}
	for {
		event, err := events.Next()
		if errors.IsCanceled(err) {
			return
		}
		if err == io.EOF {
			time.AfterFunc(time.Second, func() {
				v.watch(ctx)
			})
		}
		if err != nil {
			log.Error(err)
		} else {
			switch e := event.(type) {
			case *Created[V]:
				v.mu.Lock()
				if v.value == nil || v.value.Version < e.Value.Version {
					v.value = &e.Value
				}
				v.mu.Unlock()
			case *Updated[V]:
				v.mu.Lock()
				if v.value == nil || v.value.Version < e.NewValue.Version {
					v.value = &e.NewValue
				}
				v.mu.Unlock()
			case *Deleted[V]:
				v.mu.Lock()
				if v.value == nil || v.value.Version <= e.Value.Version {
					v.value = nil
				}
				v.mu.Unlock()
			}
		}
	}
}

func (v *cachingValue[V]) Set(ctx context.Context, value V, opts ...SetOption) (primitive.Versioned[V], error) {
	versioned, err := v.Value.Set(ctx, value, opts...)
	if err != nil {
		v.mu.Lock()
		v.value = nil
		v.mu.Unlock()
		return versioned, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.value == nil || v.value.Version < versioned.Version {
		v.value = &versioned
	}
	return versioned, err
}

func (v *cachingValue[V]) Update(ctx context.Context, value V, opts ...UpdateOption) (primitive.Versioned[V], error) {
	versioned, err := v.Value.Update(ctx, value, opts...)
	if err != nil {
		v.mu.Lock()
		v.value = nil
		v.mu.Unlock()
		return versioned, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.value == nil || v.value.Version < versioned.Version {
		v.value = &versioned
	}
	return versioned, err
}

func (v *cachingValue[V]) Get(ctx context.Context, opts ...GetOption) (primitive.Versioned[V], error) {
	v.mu.RLock()
	value := v.value
	v.mu.RUnlock()
	if value != nil {
		return *value, nil
	}

	versioned, err := v.Value.Get(ctx, opts...)
	if err != nil {
		v.mu.Lock()
		v.value = nil
		v.mu.Unlock()
		return versioned, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.value == nil || v.value.Version < versioned.Version {
		v.value = &versioned
	}
	return versioned, err
}

func (v *cachingValue[V]) Delete(ctx context.Context, opts ...DeleteOption) error {
	err := v.Value.Delete(ctx, opts...)
	v.mu.Lock()
	v.value = nil
	v.mu.Unlock()
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
