# Map

The `Map` primitive provides a distributed map that supports concurrency control through optimistic
locking. Maps store `string` keys and `[]byte` values, and map entries are represented in return
values as a `KeyValue` object with the following fields:
* `Key` - the `string` map key
* `Value` - the `[]byte` entry value
* `Version` - a monotonically increasing, unique `int64` entry version suitable for use in optimistic locks

To create a distributed map, get a `Database` and call `GetMap` on the database:

```go
myMap, err := atomix.GetMap(context.Background(), "my-map")
if err != nil {
	...
}

defer myMap.Close(context.Background())
```

To put a value in a map, call `Put`:

```go
entry, err := myMap.Put(context.Background(), "foo", []byte("bar"))
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
`IfMatch` option:

```go
entry, err := myMap.Put(context.Background(), "foo", []byte("baz"), _map.IfMatch(entry.ObjectMeta))
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
entry, err = myMap.Remove(context.Background(), "foo", _map.IfMatch(entry.ObjectMeta))
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
ch := make(chan _map.Event)
err := myMap.Watch(context.Background(), ch)
for event := range ch {
    ...
}
```
