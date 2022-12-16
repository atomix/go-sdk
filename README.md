<!--
SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
SPDX-License-Identifier: Apache-2.0 
-->

[![Snapshot](https://github.com/atomix/go-sdk/actions/workflows/build.yml/badge.svg)](https://github.com/atomix/go-sdk/actions/workflows/build.yml)
[![Release](https://github.com/atomix/go-sdk/actions/workflows/release.yml/badge.svg)](https://github.com/atomix/go-sdk/actions/workflows/release.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/atomix/go-sdk)](https://goreportcard.com/report/github.com/atomix/go-sdk)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/gojp/goreportcard/blob/master/LICENSE)
[![GoDoc](https://godoc.org/github.com/atomix/go-sdk?status.svg)](https://godoc.org/github.com/atomix/go-sdk)

# Go Client

This project provides a [Go] client for [Atomix Cloud].

## Getting Started

### Installation

To install the Go client, use `go get`:

```bash
$ GO111MODULE=on go get github.com/atomix/go-sdk
```

### Usage

To use the client, import the client API:

```go
import "github.com/atomix/go-sdk/pkg/atomix"
```

The `atomix` package provides functions for working with primitives using the default cluster configuration:

```go
counter, err := atomix.GetCounter(context.Background(), "my-counter")
```

To use a non-default configuration, create a client by calling `NewClient`:

```go
client := atomix.NewClient(atomix.WithBrokerPort(8000))
counter, err := client.GetCounter(context.Background(), "my-counter")
```

To create a distributed primitive, call the getter for the desired type, passing the name of the primitive and any
additional primitive options:

```go
lock, err := atomix.GetLock(context.Background(), "my-lock")
if err != nil {
panic(err)
}
```

Primitive names are shared across all clients for a given _scope_ within a given
_database_. Any two primitives with the same name in the same scope and stored in the same database reference the same
state machine regardless of client locations. So a
`Lock` call in one container will block will block lock requests from all other containers until unlocked.

```go
version, err := lock.Lock(context.Background())
if err == nil {
// Lock is acquired with version 'version'
}
```

When a primitive is no longer in used by the client it can be closed with `Close` to reclaim resources:

```go
lock.Close(context.Background())
```

## Counter

The `Counter` primitive is a distributed counter that supports atomic increment, decrement,
and check-and-set operations. To create a counter, call `GetCounter` on the database in which
to create the counter:

```go
myCounter, err := atomix.GetCounter(context.Background(), "my-counter")
if err != nil {
	...
}

defer myCounter.Close(context.Background())
```

```go
count, err := myCounter.Get(context.Background())
if err != nil {
	...
}
```

```go
count, err = myCounter.Set(context.Background(), 10)
if err != nil {
	...
}
```

```go
count, err = myCounter.Increment(context.Background(), 1)
if err != nil {
	...
}
```

```go
count, err = myCounter.Decrement(context.Background(), 10)
if err !=  nil {
	...
}
```

## Leader Election

The `Election` primitive supports distributed leader election. Leader elections are implemented
using first-in-first-out, but clients can sort election priority through various operations on
the `Election` interface.

To create an `Election`, call `GetElection` on the database in which to create the election:

```go
myElection, err := atomix.GetElection(context.Background(), "my-election")
if err != nil {
	...
}

defer myElection.Close(context.Background())
```

Each `Election` object has a globally unique node ID which is used to identify the client and
can be read by calling `ID()`:

```go
id := myElection.ID()
```

The election ID is used to differentiate candidates and can be explicitly assigned by specifying a
session ID when getting the election instance:

```go
myElection, err := atomix.GetElection(context.Background(), "my-election", primitive.WithSessionID("node-1"))
```

The current election `Term` can be retrieved by calling `GetTerm`:

```go
term, err := myElection.GetTerm(context.Background())
if err != nil {
	...
}
```

The `Term` contains the complete state of the election:
* `Leader` - the current leader ID
* `Term` - a `uint64` per-leader, globally unique, monotonically increasing epoch for the leader
* `Candidates` - a sequence of all candidates participating in the election in priority order,
  including the current leader

To enter the client into the election, call `Enter`:

```go
term, err = myElection.Enter(context.Background())
if err != nil {
	...
}
```

The `Enter` call will return the resulting `Term` struct which can be used to determine whether the
client won the election:

```go
if term.Leader == myElection.ID() {
	// This node is the leader
}
```

Clients can leave the election by calling `Leave`:

```go
err = myElection.Leave(context.Background())
if err != nil {
	...
}
```

When the leader leaves an election, a new leader will be elected. The `Watch` method can be used to
watch the election for changes. When the leader or candidates changes, an event will be published
to all watchers.

```go
ch := make(chan election.Event)
err := myElection.Watch(context.Background(), ch)
for event := range ch {
    ...
}
```

## IndexedMap

## List

## Lock

The `Lock` primitive is a distributed lock that provides lock version numbers for fencing.
To create a lock, call `GetLock` on the database in which to create the lock.

_Note that it's recommended distributed locking be used with a strongly consistent
database that implements a protocol like `raft`.

```go
myLock, err := atomix.GetLock(context.Background(), "my-lock")
if err != nil {
	...
}

defer myLock.Close(context.Background())
```

To acquire the lock, call `Lock`:

```go
version, err := myLock.Lock(context.Background())
if err != nil {
	...
}
```

If the lock is currently held by another client (or another `Lock` instance owned by the
current client), the `Lock()` call will block until the lock is `Unlock`ed by the owning
client. A timeout can be provided by the `Context`:

```go
ctx := context.WithTimeout(context.Background(), 10 * time.Second)
version, err := myLock.Lock(ctx)
if err != nil {
	...
}
```

Successful calls to `Lock()` return a `uint64` lock version number. The lock version number
is guaranteed to be unique and monotonically increasing, so it's suitable for fencing and
optimistic locking.

To determine whether the lock is currently held by any client, call `IsLocked`:

```go
locked, err := myLock.IsLocked(context.Background())
if err != nil {
	...
}
```

A lock version number can also be passed using `WithVersion` to determine whether the
lock is held by an owner with the given version number:

```go
locked, err = myLock.IsLocked(context.Background(), atomixlock.WithVersion(version))
if err != nil {
	...
}
```

Once the client has finished with the lock, unlock it by calling `Unlock`:

```go
unlocked, err := myLock.Unlock(context.Background())
if err != nil {
	...
}
```

Clients can also release any process's lock by passing the owner's lock version
number:

```go
unlocked, err = myLock.Unlock(context.Background(), lock.IfMatch(lock))
if err != nil {
	...
}
```

## Map

The `Map` primitive provides a distributed map that supports concurrency control through optimistic
locking. Maps store `string` keys and `[]byte` values, and map entries are represented in return
values as a `KeyValue` object with the following fields:
* `Key` - the `string` map key
* `Value` - the `[]byte` entry value
* `Version` - a monotonically increasing, unique `int64` entry version suitable for use in optimistic locks

To create a distributed map, get a `Database` and call `GetMap` on the database:

```go
myMap, err := atomix.GetMap[string, string](
	context.Background(), 
	"my-map", 
	_map.WithKeyType(generic.String()),
	_map.WithValueType(generic.String()))
if err != nil {
	...
}

defer myMap.Close(context.Background())
```

To put a value in a map, call `Put`:

```go
entry, err := myMap.Put(context.Background(), "foo", "bar")
if err != nil {
	...
}
```

The returned `Entry` contains the metadata for the entry that was written to the map. `Get` also
returns the `Entry`:

```go
entry, err = myMap.Get(context.Background(), "foo")
if err != nil {
	...
}
```

This entry metadata can be used for optimistic locking when updating the entry using the
`IfTimestamp` option:

```go
entry, err := myMap.Update(context.Background(), "foo", "baz", _map.IfTimestamp(entry.Timestamp))
if err != nil {
	...
}
```

To remove a key from the map, call `Remove`:

```go
entry, err = myMap.Remove(context.Background(), "foo")
if err != nil {
	...
}
```

Again, optimistic locking can be used when removing an entry:

```go
entry, err = myMap.Remove(context.Background(), "foo", _map.IfTimestamp(entry.Timestamp))
if err != nil {
	...
}
```

Call `Clear` to remove all entries from the map:

```go
err = myMap.Clear(context.Background())
if err != nil {
	...
}
```

The `Watch` method can be used to watch the map for changes. When the map is modified an event will be published to all watchers.

```go
ch := make(chan _map.Event[string, string])
err := myMap.Watch(context.Background(), ch)
for event := range ch {
    ...
}
```

## Set

The `Set` primitive is a partitioned distributed set. Set values are stored as `strings`. To create a set, call `GetSet`
on the database in which to create the set:

```go
mySet, err := atomix.GetSet[string](context.Background(), "my-set", set.WithElementType(generic.String()))
if err != nil {
    ...
}

defer mySet.Close(context.Background())
```

To add an element to the set, call `Add`:

```go
added, err := mySet.Add(context.Background(), "foo")
if err != nil {
    ...
}

```

To check if the set contains an element, call `Contains`:

```go
contains, err := mySet.Contains(context.Background(), "foo")
if err != nil {
    ...
}

```

And to remove an element from the set, call `Remove`:

```go
removed, err := mySet.Remove(context.Background(), "foo")
if err != nil {
    ...
}

```

Or to remove all elements from the set, call `Clear`:

```go
err := mySet.Clear(context.Background())
if err != nil {
    ...
}
```

The `Watch` method can be used to watch the set for changes. When an element is added to or removed from the set,
an event will be published to all watchers.

```go
ch := make(chan set.Event)
err := mySet.Watch(context.Background(), ch)
for event := range ch {
    ...
}
```

## Value

The `Value` primitive is a distributed `[]byte` value that supoorts atomic set-and-get and compare-and-set operations.

```go
myValue, err := atomix.GetValue[string](context.Background(), "my-value", value.WithType(generic.String()))
if err != nil {
    ...
}

defer myValue.Close(context.Background())
```

To set the value call `Set`:

```go
ts, err := myValue.Set(context.Background(), "Hello world!")
if err != nil {
    ...
}
```

To get the current value use `Get`:

```go
value, ts, err := myValue.Get(context.Background())
if err != nil {
    ...
}
```

The `Timestamp` returned by `Get` and `Set` calls contains versioning information that can be used to perform atomic
check-and-set operations using optimistic locking:

```go
if value == "Hello world!" {
    _, err := myValue.Set(context.Background(), "Goodbye world.", value.IfTimestamp(ts))
}
```

The `Watch` method can be used to watch the value for changes. Each time the value is updated, an event will be
published to all watchers.

```go
ch := make(chan value.Event)
err := myValue.Watch(context.Background(), ch)
for event := range ch {
    ...
}
```

[Go]: https://golang.org
[Atomix Cloud]: https://atomix.io
