// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	atomiccounter "github.com/atomix/go-client/pkg/atomix/atomic/counter"
	indexedmap "github.com/atomix/go-client/pkg/atomix/atomic/indexedmap"
	lock "github.com/atomix/go-client/pkg/atomix/atomic/lock"
	atomicmap "github.com/atomix/go-client/pkg/atomix/atomic/map"
	counter "github.com/atomix/go-client/pkg/atomix/counter"
	"github.com/atomix/go-client/pkg/atomix/election"
	"github.com/atomix/go-client/pkg/atomix/generic/scalar"
	"github.com/atomix/go-client/pkg/atomix/list"
	_map "github.com/atomix/go-client/pkg/atomix/map"
	"github.com/atomix/go-client/pkg/atomix/set"
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
