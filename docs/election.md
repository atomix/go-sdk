# Leader Election

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
