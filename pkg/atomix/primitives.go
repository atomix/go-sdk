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
	"github.com/atomix/go-client/pkg/atomix/set"
	"github.com/atomix/go-client/pkg/atomix/value"
)

func GetCounter(ctx context.Context, name string, opts ...counter.Option) (counter.Counter, error) {
	return client.GetCounter(getClient())(ctx, name, opts...)
}

func GetElection(ctx context.Context, name string, opts ...election.Option) (election.Election, error) {
	return client.GetElection(getClient())(ctx, name, opts...)
}

func GetIndexedMap[K, V any](ctx context.Context, name string, opts ...indexedmap.Option[K, V]) (indexedmap.IndexedMap[K, V], error) {
	return client.GetIndexedMap[K, V](getClient())(ctx, name, opts...)
}

func GetList[E any](ctx context.Context, name string, opts ...list.Option[E]) (list.List[E], error) {
	return client.GetList[E](getClient())(ctx, name, opts...)
}

func GetLock(ctx context.Context, name string, opts ...lock.Option) (lock.Lock, error) {
	return client.GetLock(getClient())(ctx, name, opts...)
}

func GetMap[K, V any](ctx context.Context, name string, opts ..._map.Option[K, V]) (_map.Map[K, V], error) {
	return client.GetMap[K, V](getClient())(ctx, name, opts...)
}

func GetSet[E any](ctx context.Context, name string, opts ...set.Option[E]) (set.Set[E], error) {
	return client.GetSet[E](getClient())(ctx, name, opts...)
}

func GetValue[E any](ctx context.Context, name string, opts ...value.Option[E]) (value.Value[E], error) {
	return client.GetValue[E](getClient())(ctx, name, opts...)
}
