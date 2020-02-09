# Counter

The `Counter` primitive is a distributed counter that supports atomic increment, decrement,
and check-and-set operations. To create a counter, call `GetCounter` on the database in which
to create the counter:

```go
db, err := client.GetDatabase(context.TODO(), "raft")
if err != nil {
	...
}

counter, err := db.GetCounter(context.TODO(), "my-counter")
if err != nil {
	...
}

defer counter.Close(context.TODO())
```

```go
count, err := counter.Get(context.TODO())
if err != nil {
	...
}
```

```go
count, err = counter.Set(context.TODO(), 10)
if err != nil {
	...
}
```

```go
count, err = counter.Increment(context.TODO(), 1)
if err != nil {
	...
}
```

```go
count, err = counter.Decrement(context.TODO(), 10)
if err !=  nil {
	...
}
```
