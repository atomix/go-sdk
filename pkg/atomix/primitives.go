// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atomix

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/client"
	"github.com/atomix/go-client/pkg/atomix/counter"
	"github.com/atomix/go-client/pkg/atomix/election"
	"github.com/atomix/go-client/pkg/atomix/indexedmap"
	"github.com/atomix/go-client/pkg/atomix/list"
	"github.com/atomix/go-client/pkg/atomix/lock"
	_map "github.com/atomix/go-client/pkg/atomix/map"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"github.com/atomix/go-client/pkg/atomix/set"
	"github.com/atomix/go-client/pkg/atomix/value"
)

func GetCounter(ctx context.Context, name string, opts ...primitive.Option) func(opts ...counter.Option) (counter.Counter, error) {
	return client.Counter(getClient()).Get(ctx, name, opts...)
}

func GetElection(ctx context.Context, name string, opts ...primitive.Option) func(opts ...election.Option) (election.Election, error) {
	return client.Election(getClient()).Get(ctx, name, opts...)
}

func GetIndexedMap[K, V any](ctx context.Context, name string, opts ...primitive.Option) func(opts ...indexedmap.Option[K, V]) (indexedmap.IndexedMap[K, V], error) {
	return client.IndexedMap[K, V](getClient()).Get(ctx, name, opts...)
}

func GetList[E any](ctx context.Context, name string, opts ...primitive.Option) func(opts ...list.Option[E]) (list.List[E], error) {
	return client.List[E](getClient()).Get(ctx, name, opts...)
}

func GetLock(ctx context.Context, name string, opts ...primitive.Option) func(opts ...lock.Option) (lock.Lock, error) {
	return client.Lock(getClient()).Get(ctx, name, opts...)
}

func GetMap[K, V any](ctx context.Context, name string, opts ...primitive.Option) func(opts ..._map.Option[K, V]) (_map.Map[K, V], error) {
	return client.Map[K, V](getClient()).Get(ctx, name, opts...)
}

func GetSet[E any](ctx context.Context, name string, opts ...primitive.Option) func(opts ...set.Option[E]) (set.Set[E], error) {
	return client.Set[E](getClient()).Get(ctx, name, opts...)
}

func GetValue[E any](ctx context.Context, name string, opts ...primitive.Option) func(opts ...value.Option[E]) (value.Value[E], error) {
	return client.Value[E](getClient()).Get(ctx, name, opts...)
}
