<!--
SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
SPDX-License-Identifier: Apache-2.0
-->

# Getting Started

## Installation

To install the Go client, use `go get`:

```bash
$ GO111MODULE=on go get github.com/atomix/go-client
```

## Usage

To use the client, import the client API:

```go
import "github.com/atomix/go-client/pkg/atomix"
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

[API]: /api

[golang]: https://golang.org/
