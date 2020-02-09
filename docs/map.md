# Map

The `Map` primitive provides a distributed map that supports concurrency control through optimistic
locking. Maps store `string` keys and `[]byte` values, and map entries are represented in return
values as a `KeyValue` object with the following fields:
* `Key` - the `string` map key
* `Value` - the `[]byte` entry value
* `Version` - a monotonically increasing, unique `int64` entry version suitable for use in optimistic locks

To create a distributed map, get a `Database` and call `GetMap` on the database:

```go
db, err := client.GetDatabase(context.TODO(), "raft")
if err != nil {
	...
}

_map, err := db.GetMap(context.TODO(), "my-map")
if err != nil {
	...
}

defer _map.Close(context.TODO())
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
