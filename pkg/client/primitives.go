// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"github.com/atomix/go-client/pkg/generic/scalar"
	atomiccounter "github.com/atomix/go-client/pkg/primitive/atomic/counter"
	indexedmap "github.com/atomix/go-client/pkg/primitive/atomic/indexedmap"
	lock "github.com/atomix/go-client/pkg/primitive/atomic/lock"
	atomicmap "github.com/atomix/go-client/pkg/primitive/atomic/map"
	counter "github.com/atomix/go-client/pkg/primitive/counter"
	"github.com/atomix/go-client/pkg/primitive/election"
	"github.com/atomix/go-client/pkg/primitive/list"
	_map "github.com/atomix/go-client/pkg/primitive/map"
	"github.com/atomix/go-client/pkg/primitive/set"
)

func AtomicCounter(client *Client) func(name string) *atomiccounter.Builder {
	return func(name string) *atomiccounter.Builder {
		return atomiccounter.NewBuilder(client, name)
	}
}

func AtomicMap[K scalar.Scalar, V any](client *Client) func(name string) *atomicmap.Builder[K, V] {
	return func(name string) *atomicmap.Builder[K, V] {
		return atomicmap.NewBuilder[K, V](client, name)
	}
}

func IndexedMap[K scalar.Scalar, V any](client *Client) func(name string) *indexedmap.Builder[K, V] {
	return func(name string) *indexedmap.Builder[K, V] {
		return indexedmap.NewBuilder[K, V](client, name)
	}
}

func Lock(client *Client) func(name string) *lock.Builder {
	return func(name string) *lock.Builder {
		return lock.NewBuilder(client, name)
	}
}

func Counter(client *Client) func(name string) *counter.Builder {
	return func(name string) *counter.Builder {
		return counter.NewBuilder(client, name)
	}
}

func LeaderElection(client *Client) func(name string) *election.Builder {
	return func(name string) *election.Builder {
		return election.NewBuilder(client, name)
	}
}

func List[E any](client *Client) func(name string) *list.Builder[E] {
	return func(name string) *list.Builder[E] {
		return list.NewBuilder[E](client, name)
	}
}

func Map[K scalar.Scalar, V any](client *Client) func(name string) *_map.Builder[K, V] {
	return func(name string) *_map.Builder[K, V] {
		return _map.NewBuilder[K, V](client, name)
	}
}

func Set[E any](client *Client) func(name string) *set.Builder[E] {
	return func(name string) *set.Builder[E] {
		return set.NewBuilder[E](client, name)
	}
}
