// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"github.com/atomix/go-sdk/pkg/util/cache"
	"io"
)

func newCachingValue(value Value[[]byte]) (Value[[]byte], error) {
	cm := &cachingValue{
		Value: value,
		cache: cache.NewValueCache[primitive.Versioned[[]byte]](),
	}
	if err := cm.open(); err != nil {
		return nil, err
	}
	return cm, nil
}

type cachingValue struct {
	Value[[]byte]
	cache   *cache.ValueCache[primitive.Versioned[[]byte]]
	closeCh chan struct{}
}

func (v *cachingValue) open() error {
	ctx, cancel := context.WithCancel(context.Background())
	v.closeCh = make(chan struct{})
	go func() {
		<-v.closeCh
		cancel()
	}()

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
				case *Created[[]byte]:
					v.cache.Store(e.Value, func(stored primitive.Versioned[[]byte]) bool {
						return e.Value.Version == 0 || e.Value.Version > stored.Version
					})
				case *Updated[[]byte]:
					v.cache.Store(e.Value, func(stored primitive.Versioned[[]byte]) bool {
						return e.Value.Version == 0 || e.Value.Version > stored.Version
					})
				case *Deleted[[]byte]:
					v.cache.Delete(func(stored primitive.Versioned[[]byte]) bool {
						return e.Value.Version == 0 || e.Value.Version > stored.Version
					})
				}
			}
		}
	}()
	return nil
}

func (v *cachingValue) Set(ctx context.Context, value []byte, opts ...SetOption) (primitive.Versioned[[]byte], error) {
	versioned, err := v.Value.Set(ctx, value, opts...)
	if err != nil {
		return versioned, err
	}
	v.cache.Store(versioned, func(stored primitive.Versioned[[]byte]) bool {
		return versioned.Version == 0 || versioned.Version > stored.Version
	})
	return versioned, err
}

func (v *cachingValue) Update(ctx context.Context, value []byte, opts ...UpdateOption) (primitive.Versioned[[]byte], error) {
	versioned, err := v.Value.Update(ctx, value, opts...)
	if err != nil {
		return versioned, err
	}
	v.cache.Store(versioned, func(stored primitive.Versioned[[]byte]) bool {
		return versioned.Version == 0 || versioned.Version > stored.Version
	})
	return versioned, err
}

func (v *cachingValue) Get(ctx context.Context, opts ...GetOption) (primitive.Versioned[[]byte], error) {
	if value, ok := v.cache.Load(); ok {
		return value, nil
	}

	value, err := v.Value.Get(ctx, opts...)
	if err != nil {
		return primitive.Versioned[[]byte]{}, err
	}
	v.cache.Store(value, func(stored primitive.Versioned[[]byte]) bool {
		return value.Version == 0 || value.Version > stored.Version
	})
	return value, err
}

func (v *cachingValue) Delete(ctx context.Context, opts ...DeleteOption) error {
	err := v.Value.Delete(ctx, opts...)
	v.cache.Delete(func(stored primitive.Versioned[[]byte]) bool {
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (v *cachingValue) Events(ctx context.Context, opts ...EventsOption) (EventStream[[]byte], error) {
	events, err := v.Value.Events(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return stream.NewInterceptingStream[Event[[]byte]](events, func(event Event[[]byte]) {
		switch e := event.(type) {
		case *Created[[]byte]:
			v.cache.Store(e.Value, func(stored primitive.Versioned[[]byte]) bool {
				return e.Value.Version == 0 || e.Value.Version > stored.Version
			})
		case *Updated[[]byte]:
			v.cache.Store(e.Value, func(stored primitive.Versioned[[]byte]) bool {
				return e.Value.Version == 0 || e.Value.Version > stored.Version
			})
		case *Deleted[[]byte]:
			v.cache.Delete(func(stored primitive.Versioned[[]byte]) bool {
				return e.Value.Version == 0 || e.Value.Version > stored.Version
			})
		}
	}), nil
}

func (v *cachingValue) Close(ctx context.Context) error {
	defer close(v.closeCh)
	return v.Value.Close(ctx)
}
