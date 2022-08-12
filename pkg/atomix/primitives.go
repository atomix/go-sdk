// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atomix

import (
	atomiccounter "github.com/atomix/go-client/pkg/atomix/atomic/counter"
	"github.com/atomix/go-client/pkg/atomix/atomic/indexedmap"
	"github.com/atomix/go-client/pkg/atomix/atomic/lock"
	atomicmap "github.com/atomix/go-client/pkg/atomix/atomic/map"
	"github.com/atomix/go-client/pkg/atomix/client"
	"github.com/atomix/go-client/pkg/atomix/counter"
	"github.com/atomix/go-client/pkg/atomix/election"
	"github.com/atomix/go-client/pkg/atomix/generic/scalar"
	"github.com/atomix/go-client/pkg/atomix/list"
	_map "github.com/atomix/go-client/pkg/atomix/map"
	"github.com/atomix/go-client/pkg/atomix/set"
)

func AtomicCounter(name string) *atomiccounter.Builder {
	return client.AtomicCounter(getClient())(name)
}

func AtomicMap[K scalar.Scalar, V any](name string) *atomicmap.Builder[K, V] {
	return client.AtomicMap[K, V](getClient())(name)
}

func IndexedMap[K scalar.Scalar, V any](name string) *indexedmap.Builder[K, V] {
	return client.IndexedMap[K, V](getClient())(name)
}

func Lock(name string) *lock.Builder {
	return client.Lock(getClient())(name)
}

func Counter(name string) *counter.Builder {
	return client.Counter(getClient())(name)
}

func LeaderElection(name string) *election.Builder {
	return client.LeaderElection(getClient())(name)
}

func List[E any](name string) *list.Builder[E] {
	return client.List[E](getClient())(name)
}

func Map[K scalar.Scalar, V any](name string) *_map.Builder[K, V] {
	return client.Map[K, V](getClient())(name)
}

func Set[E any](name string) *set.Builder[E] {
	return client.Set[E](getClient())(name)
}
