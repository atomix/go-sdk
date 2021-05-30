# Value

The `Value` primitive is a distributed `[]byte` value that supoorts atomic set-and-get and compare-and-set operations.

```go
myValue, err := atomix.GetValue(context.Background(), "my-value")
if err != nil {
    ...
}

defer myValue.Close(context.Background())
```

To set the value call `Set`:

```go
meta, err := myValue.Set(context.Background(), []byte("Hello world!"))
if err != nil {
    ...
}
```

To get the current value use `Get`:

```go
bytes, meta, err := myValue.Get(context.Background())
if err != nil {
    ...
}
```

The `ObjectMeta` returned by `Get` and `Set` calls contains versioning information that can be used to perform atomic
check-and-set operations using optimistic locking:

```go
if string(bytes) == "Hello world!" {
    _, err := myValue.Set(context.Background(), []byte("Goodbye world."), value.IfMatch(meta))
}
```

The `Watch` method can be used to watch the value for changes. Each time the value is updated, an event will be
published to all watchers.

```go
ch := make(chan value.Event)
err := myValue.Watch(context.Background(), ch)
for event := range ch {
    ...
}
```
