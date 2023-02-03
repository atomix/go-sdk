// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atomix

import (
	"github.com/atomix/go-sdk/pkg/client"
	"github.com/atomix/go-sdk/pkg/primitive/counter"
	"github.com/atomix/go-sdk/pkg/primitive/election"
	"github.com/atomix/go-sdk/pkg/primitive/indexedmap"
	"github.com/atomix/go-sdk/pkg/primitive/list"
	"github.com/atomix/go-sdk/pkg/primitive/lock"
	_map "github.com/atomix/go-sdk/pkg/primitive/map"
	"github.com/atomix/go-sdk/pkg/primitive/set"
	"github.com/atomix/go-sdk/pkg/primitive/value"
	"github.com/atomix/go-sdk/pkg/types/scalar"
)

func Counter(name string) counter.Builder {
	return client.Counter(getClient())(name)
}

func IndexedMap[K scalar.Scalar, V any](name string) indexedmap.Builder[K, V] {
	return client.IndexedMap[K, V](getClient())(name)
}

func LeaderElection(name string) election.Builder {
	return client.LeaderElection(getClient())(name)
}

func List[E any](name string) list.Builder[E] {
	return client.List[E](getClient())(name)
}

func Lock(name string) *lock.Builder {
	return client.Lock(getClient())(name)
}

func Map[K scalar.Scalar, V any](name string) _map.Builder[K, V] {
	return client.Map[K, V](getClient())(name)
}

func Set[E any](name string) set.Builder[E] {
	return client.Set[E](getClient())(name)
}

func Value[V any](name string) value.Builder[V] {
	return client.Value[V](getClient())(name)
}
