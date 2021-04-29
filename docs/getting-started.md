# Getting Started

## Installation

To install the Go client, use `go get`:

```bash
$ GO111MODULE=on go get github.com/atomix/atomix-go-client
```

## Usage

To create a client call `client.New`, passing the controller host and port:

```go
import "github.com/atomix/atomix-go-client/pkg/client"

atomix, err := client.New("atomix-controller.kube-system.svc.cluster.local:5679")
if err != nil {
	panic(err)
}
```

The client can optionally be configured with a Kubernetes _namespace_ and application
_scope_:

```go
atomix, err := client.New(
	"atomix-controller.kube-system.svc.cluster.local:5679", 
	client.WithNamespace("default"), 
	client.WithScope("my-app"))
```

The `client.Client` API cannot itself create distributed primitives. To create primitives you
must first get an instance of a deployed `Database`:

```go
db, err := atomix.GetDatabase(context.TODO(), "raft")
if err != nil {
	panic(err)
}
```

The `Database` API provides getters for each distributed primitive type:

* `GetCounter` creates or gets a named distributed [counter](counter.md)
* `GetElection` creates or gets a named distributed [leader election](election.md)
* `GetIndexedMap` creates or gets a named distributed [indexed map](indexed-map.md)
* `GetLeaderLatch` creates or gets a named distributed [leader latch](leader-latch.md)
* `GetList` creates or gets a named distributed [list](list.md)
* `GetLock` creates or gets a named distributed [lock](lock.md)
* `GetLog` creates or gets a named distributed [log](log.md)
* `GetMap` creates or gets a named distributed [map](map.md)
* `GetSet` creates or gets a named distributed [set](set.md)
* `GetValue` creates or gets a named distributed [value](value.md)

To create a distributed primitive, call the getter for the desired type, passing the
name of the primitive and any additional primitive options:

```go
lock, err := db.GetLock(context.TODO(), "my-lock")
if err != nil {
	panic(err)
}
```

Primitive names are shared across all clients for a given _scope_ within a given
_database_. Any two primitives with the same name in the same scope and stored in the 
same database reference the same state machine regardless of client locations. So a
`Lock` call in one container will block will block lock requests from all other containers
until unlocked.

```go
version, err := lock.Lock(context.TODO())
if err == nil {
	// Lock is acquired with version 'version'
}
```

When a primitive is no longer in used by the client it can be closed with `Close` to
reclaim resources:

```go
lock.Close(context.TODO())
```

Primitives can also be deleted by calling `Delete`:

```go
lock.Delete(context.TODO())
```

When a primitive is deleted, all other open instances of the primitive will be closed
and the state of the primitive will be permanently discarded.

[API]: /api
[golang]: https://golang.org/
