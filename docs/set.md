# Set

The `Set` primitive is a partitioned distributed set. Set values are stored as `strings`. To
create a set, call `GetSet` on the database in which to create the set:

```go
db, err := client.GetDatabase(context.TODO(), "raft")
if err != nil {
	...
}

set, err := db.GetSet(context.TODO(), "my-set")
if err != nil {
	...
}

defer set.Close(context.TODO())
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
