// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"github.com/atomix/go-sdk/pkg/primitive"
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

func Counter(client primitive.Client) func(name string) counter.Builder {
	return func(name string) counter.Builder {
		return counter.NewBuilder(client, name)
	}
}

func IndexedMap[K scalar.Scalar, V any](client primitive.Client) func(name string) *indexedmap.Builder[K, V] {
	return func(name string) *indexedmap.Builder[K, V] {
		return indexedmap.NewBuilder[K, V](client, name)
	}
}

func LeaderElection(client primitive.Client) func(name string) *election.Builder {
	return func(name string) *election.Builder {
		return election.NewBuilder(client, name)
	}
}

func List[E any](client primitive.Client) func(name string) *list.Builder[E] {
	return func(name string) *list.Builder[E] {
		return list.NewBuilder[E](client, name)
	}
}

func Lock(client primitive.Client) func(name string) *lock.Builder {
	return func(name string) *lock.Builder {
		return lock.NewBuilder(client, name)
	}
}

func Map[K scalar.Scalar, V any](client primitive.Client) func(name string) *_map.Builder[K, V] {
	return func(name string) *_map.Builder[K, V] {
		return _map.NewBuilder[K, V](client, name)
	}
}

func Set[E any](client primitive.Client) func(name string) set.Builder[E] {
	return func(name string) set.Builder[E] {
		return set.NewBuilder[E](client, name)
	}
}

func Value[V any](client primitive.Client) func(name string) *value.Builder[V] {
	return func(name string) *value.Builder[V] {
		return value.NewBuilder[V](client, name)
	}
}
