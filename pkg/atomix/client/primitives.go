// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
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

func Counter(client *Client) primitive.Provider[counter.Counter, counter.Option] {
	return counter.Provider(client)
}

func Election(client *Client) primitive.Provider[election.Election, election.Option] {
	return election.Provider(client)
}

func IndexedMap[K, V any](client *Client) primitive.Provider[indexedmap.IndexedMap[K, V], indexedmap.Option[K, V]] {
	return indexedmap.Provider[K, V](client)
}

func List[E any](client *Client) primitive.Provider[list.List[E], list.Option[E]] {
	return list.Provider[E](client)
}

func Lock(client *Client) primitive.Provider[lock.Lock, lock.Option] {
	return lock.Provider(client)
}

func Map[K, V any](client *Client) primitive.Provider[_map.Map[K, V], _map.Option[K, V]] {
	return _map.Provider[K, V](client)
}

func Set[E any](client *Client) primitive.Provider[set.Set[E], set.Option[E]] {
	return set.Provider[E](client)
}

func Value[V any](client *Client) primitive.Provider[value.Value[V], value.Option[V]] {
	return value.Provider[V](client)
}
