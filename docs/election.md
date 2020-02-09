# Leader Election

The `Election` primitive supports distributed leader election. Leader elections are implemented
using first-in-first-out, but clients can sort election priority through various operations on
the `Election` interface.

To create an `Election`, call `GetElection` on the database in which to create the election:

```go
db, err := client.GetDatabase(context.TODO(), "raft")
if err != nil {
	...
}

election, err := db.GetElection(context.TODO(), "my-election")
if err != nil {
	...
}

defer election.Close(context.TODO())
```

Each `Election` object has a globally unique node ID which is used to identify the client and
can be read by calling `ID()`:

```go
id := election.ID()
```

The election ID is used to differentiate candidates and can be explicitly assigned by passing additional
options to the election getter:

```go
election, err := database.GetElection(context.TODO(), "my-election", election.WithID("node-1"))
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
