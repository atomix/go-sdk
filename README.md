# Atomix Go Client

This project provides a [Go](https://golang.org) client for
[Atomix](https://atomix.io).

## Setup
To build the protocol buffers, run `make build`. The build step requires
that you have Docker installed.

## Usage
To pull the project run `go get -u github.com/atomix/atomix-go`

To create a new client, use the `New` function:

```go
import (
	atomix "github.com/atomix/atomix-go-client/pkg/client"
)

client, err := atomix.New("atomix-controller.kube-system.svc.cluster.local:5679")
if err != nil {
	...
}
```

Once the client has been created, it can be used to access primitives through partition groups:

```go
group, err := client.GetGroup(context.TODO(), "raft")
if err != nil {
	...
}

_map, err := group.GetMap(context.TODO(), "my-map")
if err != nil {
	...
}

value, err := _map.Put(context.TODO(), "foo", []byte("bar"))
if err != nil {
	...
}

value, err = _map.Get(context.TODO(), "foo")
if err != nil {
	...
}
```

The client can also be used to manage partition groups controlled by the connected controller:

```go
group, err := client.CreateGroup(context.TODO(), "raft", 3, 3, raft.Protocol{})
if err != nil {
	...
}

err = client.DeleteGroup(context.TODO(), "raft")
if err != nil {
	...
}
```
