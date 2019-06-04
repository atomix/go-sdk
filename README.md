# Atomix Go Client

This project provides a [Go] client for [Atomix].

## Client Usage
To pull the project run `go get -u github.com/atomix/atomix-go`

The `client` package provides the primary APIs to managing Atomix partitions and primitives.
Connect to the Atomix controller by creating a new client and providing the controller address:

```go
import (
	atomixclient "github.com/atomix/atomix-go-client/pkg/client"
)

client, err := atomixclient.New("atomix-controller.kube-system.svc.cluster.local:5679")
if err != nil {
	...
}
```

The client can optionally be configured with a _namespace_ within which to operate on
partition groups. If no namespace is specified, the `default` namespace will be used:

```go
client, err := atomixclient.New("atomix-controller:5679", client.WithNamespace("prod"))
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
client, err := atomixclient.New("atomix-controller:5679", client.WithApplication("my-service"))
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

### Map

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

value, err := _map.Put(context.TODO(), "foo", []byte("bar"))
if err != nil {
	...
}

value, err := _map.Put(context.TODO(), "foo", []byte("baz"), atomixmap.WithVersion(value.Version))
if err != nil {
	...
}

value, err = _map.Get(context.TODO(), "foo")
if err != nil {
	...
}

value, err = _map.Remove(context.TODO(), "foo", atomixmap.WithVersion(value.Version))
if err != nil {
	...
}

size, err := _map.Size(context.TODO())
if err != nil {
	...
}

value, err = _map.Remove(context.TODO(), "foo")
if err != nil {
	...
}

value, err = _map.Get(context.TODO(), "foo", atomixmap.WithDefault([]byte("bar")))
if err != nil {
	...
}

err = _map.Clear(context.TODO())
if err != nil {
	...
}
```

```go
ch := make(chan *_map.MapEvent)
err := m.Listen(context.TODO(), ch)
for event := range ch {
	...
}
```

### Set

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

added, err := set.Add(context.TODO(), "foo")
if err != nil {
	...
}

contains, err := set.Contains(context.TODO(), "foo")
if err != nil {
	...
}

removed, err := set.Remove(context.TODO(), "foo")
if err != nil {
	...
}

size, err := set.Size(context.TODO())
if err != nil {
	...
}
```

```go
ch := make(chan *set.SetEvent)
err := set.Listen(context.TODO(), ch)
for event := range ch {
	...
}
```

### Counter

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

count, err := counter.Get(context.TODO())
if err != nil {
	...
}

count, err = counter.Set(context.TODO(), 10)
if err != nil {
	...
}

count, err = counter.Increment(context.TODO(), 1)
if err != nil {
	...
}

count, err = counter.Decrement(context.TODO(), 10)
if err !=  nil {
	...
}
```

### Lock

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

version, err := lock.Lock(context.TODO())
if err != nil {
	...
}

locked, err := lock.IsLocked(context.TODO())
if err != nil {
	...
}

locked, err = lock.IsLocked(context.TODO(), atomixlock.WithVersion(version))
if err != nil {
	...
}

unlocked, err := lock.Unlock(context.TODO())
if err != nil {
	...
}

unlocked, err = lock.Unlock(context.TODO(), atomixlock.WithVersion(version))
if err != nil {
	...
}
```

### Leader Election

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

term, err := election.GetTerm(context.TODO())
if err != nil {
	...
}

term, err = election.Enter(context.TODO())
if err != nil {
	...
}

if term.Leader == election.Id {
	...
}

err = election.Leave(context.TODO())
if err != nil {
	...
}

term, err = election.GetTerm(context.TODO())
if err != nil {
	...
}

ok, err := election.Anoint(context.TODO(), term.Candidates[1])
if err != nil {
	...
}

ok, err = election.Evict(context.TODO(), term.Leader)
if err != nil {
	...
}
```

```go
ch := make(chan *election.ElectionEvent)
election.Listen(context.TODO(), ch)
for event := range ch {
	...
}
```

## Partition Groups

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
