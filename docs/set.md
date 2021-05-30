# Set

The `Set` primitive is a partitioned distributed set. Set values are stored as `strings`. To create a set, call `GetSet`
on the database in which to create the set:

```go
mySet, err := atomix.GetSet(context.Background(), "my-set")
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
