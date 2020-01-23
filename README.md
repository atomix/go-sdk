# Atomix Go Client

[![Build Status](https://travis-ci.org/atomix/go-client.svg?branch=master)](https://travis-ci.org/atomix/go-client)
[![Integration Test Status](https://img.shields.io/travis/atomix/go-client?label=Integration%20Tests&logo=Integration)](https://travis-ci.org/onosproject/onos-test)
[![Go Report Card](https://goreportcard.com/badge/github.com/atomix/go-client)](https://goreportcard.com/report/github.com/atomix/go-client)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/gojp/goreportcard/blob/master/LICENSE)
[![Coverage Status](https://img.shields.io/coveralls/github/atomix/go-client/badge.svg)](https://coveralls.io/github/atomix/go-client?branch=master)
[![GoDoc](https://godoc.org/github.com/atomix/go-client?status.svg)](https://godoc.org/github.com/atomix/go-client)

This project provides a [Go] client for [Atomix].

## Table of Contents
1. [Client Usage](#client-usage)
2. [Distributed Primitives](#distributed-primitives)
   * [Map](#map)
   * [Set](#set)
   * [Counter](#counter)
   * [Lock](#lock)
   * [Leader Election](#leader-election)
3. [Partition Group Management](#managing-partition-groups)

## Client Usage
To pull the project run `go get -u github.com/atomix/atomix-go`

The `client` package provides the primary APIs to managing Atomix partitions and primitives.
Connect to the Atomix controller by creating a new client and providing the controller address:

```go
import (
	atomixclient "github.com/atomix/go-client/pkg/client"
)

client, err := atomixclient.NewClient("atomix-controller.kube-system.svc.cluster.local:5679")
if err != nil {
	...
}
```

The client can optionally be configured with a _namespace_ within which to operate on
partition groups. If no namespace is specified, the `default` namespace will be used:

```go
client, err := atomixclient.NewClient("atomix-controller:5679", atomixclient.WithNamespace("prod"))
if err != nil {
	...
}
```

The interpretation of the namespace is unspecified in the Atomix API and is thus dependent
on the controller implementation. The [Kubernetes controller][k8s-controller], for example,
maps namespaces directly onto k8s resource namespaces, so partition groups are created in
the Kubernetes namespaces provided by the configured _namespace_. Other controllers may
behave differently or event ignore namespaces altogether.

Clients can also optionally specify an _application_ name. The application name is used as
a namespace for distributed primitives, allowing primitives created by different applications
to be isolated from one another even when they exist within the same partition group and
namespace:

```go
client, err := atomixclient.NewClient("atomix-controller:5679", atomixclient.WithApplication("my-service"))
if err != nil {
	...
}
```

### Partition Groups

The client can be used to access primitives through partition groups. To get a partition
group call `GetGroup` on the client:

```go
group, err := client.GetGroup(context.TODO(), "raft")
if err != nil {
	...
}
```

The client also provides an alternative group connection method that does not rely on the controller
at all. The client can connect directly to a partition group, using DNS to lookup SRV records
to resolve partitions:

```go
group, err := client.NewGroup("atomix-raft.default.svc.cluster.local")
if err != nil {
	...
}
```

The group API can be used to create [distributed primitives](#distributed-primitives) for
replicating state: 

```go
_map, err := group.GetMap(context.TODO(), "my-map")
if err != nil {
	...
}

value, err := _map.Put(context.TODO(), "foo", []byte("bar"))
if err != nil {
	...
}
```

## Distributed Primitives

Distributed primitives are a collection of client APIs for operating on replicated state machines.
When a distributed primitive is created via a `PartitionGroup` object, a replicated state machine
backing the primitive client is created within the partition group where the primitive state is
replicated using the configured state machine replication protocol.

When a distributed primitive is created, one or more sessions is opened to partitions in the partition
group in which the primitive was created. Sessions are used to order requests, responses, and events
between the client and server for consistency. For partitioned data structures like [Map](#map) and [Set](#set),
a session is opened to each partition in the group. For consistent, ordered primitives like [Counter](#counter),
[Lock](#lock), and [Election](#leader-election), a single session is opened to one of the partitions in the
group. The partition selected by single partition primitives is guaranteed to be consistent across instances
of the primitive.

Because distributed primitives open sessions, once the client is done using a primitive is should clean
up the sessions to free resources. This can be done by calling `Close()` on the primitive:

```go
counter, err := ...

...

err = counter.Close()
if err != nil {
	...
}
```

When a primitive is closed, its state will still be persisted inside the partition(s) to which it connected.
To delete the primitive's state from the cluster, call `Delete()` instead:

```go
counter, err := ...

...

err = counter.Delete()
if err != nil {
	...
}
```

Note that when a primitive is deleted, the state for all instances of the primitive across all connected
clients will be deleted, and all existing sessions will be closed, resulting in errors on remaining clients.

### Map

The `Map` primitive provides a distributed map that supports concurrency control through optimistic
locking. Maps store `string` keys and `[]byte` values, and map entries are represented in return
values as a `KeyValue` object with the following fields:
* `Key` - the `string` map key
* `Value` - the `[]byte` entry value
* `Version` - a monotonically increasing, unique `int64` entry version suitable for use in optimistic locks

To create a distributed map, get a `PartitionGroup` and call `GetMap` on the group:

```go
group, err := client.GetGroup(context.TODO(), "raft")
if err != nil {
	...
}

_map, err := group.GetMap(context.TODO(), "my-map")
if err != nil {
	...
}

defer _map.Close()
```

To put a value in a map, call `Put`:

```go
value, err := _map.Put(context.TODO(), "foo", []byte("bar"))
if err != nil {
	...
}
```

The returned `KeyValue` contains the `Version` of the entry that was written to the map. `Get` also
returns the `KeyValue`:

```go
value, err = _map.Get(context.TODO(), "foo")
if err != nil {
	...
}
```

This entry `Version` can be used for optimistic locking when updating the entry using the
`WithVersion` option:

```go
value, err := _map.Put(context.TODO(), "foo", []byte("baz"), atomixmap.WithVersion(value.Version))
if err != nil {
	...
}
```

To remove a key from the map, call `Remove`:

```go
value, err = _map.Remove(context.TODO(), "foo")
if err != nil {
	...
}
```

Again, optimistic locking can be used when removing an entry:

```go
value, err = _map.Remove(context.TODO(), "foo", atomixmap.WithVersion(value.Version))
if err != nil {
	...
}
```

Call `Clear` to remove all entries from the map:

```go
err = _map.Clear(context.TODO())
if err != nil {
	...
}
```

Clients can also listen for update events from other clients by passing a `chan *MapEvent` to
`Listen`:

```go
ch := make(chan *_map.MapEvent)
err := m.Listen(context.TODO(), ch)
for event := range ch {
	...
}
```

Events read from the channel are guaranteed to be read in the order in which they occurred within 
the partition from which they were produced. For example, if key `foo` is set to `bar` and then 
to `baz`, _every client_ is guaranteed to see the event indicating the update to `bar` before `baz`.

### Set

The `Set` primitive is a partitioned distributed set. Set values are stored as `strings`. To
create a set, call `GetSet` on the group in which to create the set:

```go
group, err := client.GetGroup(context.TODO(), "raft")
if err != nil {
	...
}

set, err := group.GetSet(context.TODO(), "my-set")
if err != nil {
	...
}

defer set.Close()
```

To add an element to the set, call `Add`:

```go
added, err := set.Add(context.TODO(), "foo")
if err != nil {
	...
}

```

To check if the set contains an element, call `Contains`:

```go
contains, err := set.Contains(context.TODO(), "foo")
if err != nil {
	...
}

```

And to remove an element from the set, call `Remove`:

```go
removed, err := set.Remove(context.TODO(), "foo")
if err != nil {
	...
}

```

Or to remove all elements from the set, call `Clear`:

```go
err := set.Clear(context.TODO())
if err != nil {
	...
}
```

The `Set` primitive supports update events by passing a `chan *SetEvent` to `Listen`. Set events 
provide the same reliability and consistency guarantees as described for [Map](#map):

```go
ch := make(chan *set.SetEvent)
err := set.Listen(context.TODO(), ch)
for event := range ch {
	...
}
```

### Counter

The `Counter` primitive is a distributed counter that supports atomic increment, decrement,
and check-and-set operations. To create a counter, call `GetCounter` on the group in which
to create the counter:

```go
group, err := client.GetGroup(context.TODO(), "raft")
if err != nil {
	...
}

counter, err := group.GetCounter(context.TODO(), "my-counter")
if err != nil {
	...
}

defer counter.Close()
```

```go
count, err := counter.Get(context.TODO())
if err != nil {
	...
}
```

```go
count, err = counter.Set(context.TODO(), 10)
if err != nil {
	...
}
```

```go
count, err = counter.Increment(context.TODO(), 1)
if err != nil {
	...
}
```

```go
count, err = counter.Decrement(context.TODO(), 10)
if err !=  nil {
	...
}
```

### Lock

The `Lock` primitive is a distributed lock that provides lock version numbers for fencing.
To create a lock, call `GetLock` on the group in which to create the lock.

_Note that it's recommended distributed locking be used with a strongly consistent partition 
group that implements a protocol like `raft`.

```go
group, err := client.GetGroup(context.TODO(), "raft")
if err != nil {
	...
}

lock, err := group.GetLock(context.TODO(), "my-lock")
if err != nil {
	...
}

defer lock.Close()
```

To acquire the lock, call `Lock`:

```go
version, err := lock.Lock(context.TODO())
if err != nil {
	...
}
```

If the lock is currently held by another client (or another `Lock` instance owned by the
current client), the `Lock()` call will block until the lock is `Unlock`ed by the owning
client. A timeout can be provided by the `Context`:

```go
ctx := context.WithTimeout(context.Background(), 10 * time.Second)
version, err := lock.Lock(ctx)
if err != nil {
	...
}
```

Successful calls to `Lock()` return a `uint64` lock version number. The lock version number
is guaranteed to be unique and monotonically increasing, so it's suitable for fencing and
optimistic locking.

To determine whether the lock is currently held by any client, call `IsLocked`:

```go
locked, err := lock.IsLocked(context.TODO())
if err != nil {
	...
}
```

A lock version number can also be passed using `WithVersion` to determine whether the
lock is held by an owner with the given version number:

```go
locked, err = lock.IsLocked(context.TODO(), atomixlock.WithVersion(version))
if err != nil {
	...
}
```

Once the client has finished with the lock, unlock it by calling `Unlock`:

```go
unlocked, err := lock.Unlock(context.TODO())
if err != nil {
	...
}
```

Clients can also release any process's lock by passing the owner's lock version
number:

```go
unlocked, err = lock.Unlock(context.TODO(), atomixlock.WithVersion(version))
if err != nil {
	...
}
```

### Leader Election

The `Election` primitive supports distributed leader election. Leader elections are implemented
using first-in-first-out, but clients can sort election priority through various operations on
the `Election` interface.

To create an `Election`, call `GetElection` on the group in which to create the election:

```go
group, err := client.GetGroup(context.TODO(), "raft")
if err != nil {
	...
}

election, err := group.GetElection(context.TODO(), "my-election")
if err != nil {
	...
}

defer election.Close()
```

Each `Election` object has a globally unique node ID which is used to identify the client and
can be read by calling `Id()`:

```go
id := election.Id()
```

The current election `Term` can be retrieved by calling `GetTerm`:

```go
term, err := election.GetTerm(context.TODO())
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
term, err = election.Enter(context.TODO())
if err != nil {
	...
}
```

The `Enter` call will return the resulting `Term` struct which can be used to determine whether the
client won the election:

```go
if term.Leader == election.Id() {
	// This node is the leader
}
```

Clients can leave the election by calling `Leave`:

```go
err = election.Leave(context.TODO())
if err != nil {
	...
}
```

When the leader leaves an election, a new leader will be elected. Clients can receive election
event notifications by passing a `chan *ElectionEvent` to `Listen`:

```go
ch := make(chan *election.ElectionEvent)
election.Listen(context.TODO(), ch)
for event := range ch {
	...
}
```

Election events are guaranteed to be read from the channel in the order in which they occurred
in the Atomix cluster. So if node `a` is elected leader before node `b`, all clients will
receive a leader change event for node `a` before node `b`.

## Managing Partition Groups

The client API can also be used to manage partition groups controlled by the Atomix controller.
To create a new group, simply call `CreateGroup` on the client:

```go
group, err := client.CreateGroup(context.TODO(), "raft", 3, 3, raft.Protocol{})
if err != nil {
	...
}
```

When creating a partition group, the `CreateGroup` call requires the following parameters:
* The name of the partition group
* The number of partitions to create
* The number of nodes within each partition
* A protocol to run within each partition

Protocols are provided by the `protocol` package. Currently, two protocols are supported
by the Go client:
* `raft.Protocol` - the [Raft consensus protocol][Raft]
* `log.Protocol` - a primary-backup based persistent distributed log replication protocol

Partition groups can also be deleted by calling `DeleteGroup`:

```go
err = client.DeleteGroup(context.TODO(), "raft")
if err != nil {
	...
}
```

[go]: https://golang.org
[atomix]: https://atomix.io
[k8s-controller]: https://github.com/atomix/atomix-k8s-controller
[Raft]: https://raft.github.io/
