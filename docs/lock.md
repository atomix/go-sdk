# Lock

The `Lock` primitive is a distributed lock that provides lock version numbers for fencing.
To create a lock, call `GetLock` on the database in which to create the lock.

_Note that it's recommended distributed locking be used with a strongly consistent 
database that implements a protocol like `raft`.

```go
db, err := client.GetDatabase(context.TODO(), "raft")
if err != nil {
	...
}

lock, err := db.GetLock(context.TODO(), "my-lock")
if err != nil {
	...
}

defer lock.Close(context.TODO())
```

To acquire the lock, call `Lock`:

```go
version, err := lock.Lock(context.TODO())
if err != nil {
	...
}
```

If the lock is currently held by another client (or another `Lock` instance owned by the
current client), the `Lock()` call will block until the lock is `Unlock`ed by the owning
client. A timeout can be provided by the `Context`:

```go
ctx := context.WithTimeout(context.Background(), 10 * time.Second)
version, err := lock.Lock(ctx)
if err != nil {
	...
}
```

Successful calls to `Lock()` return a `uint64` lock version number. The lock version number
is guaranteed to be unique and monotonically increasing, so it's suitable for fencing and
optimistic locking.

To determine whether the lock is currently held by any client, call `IsLocked`:

```go
locked, err := lock.IsLocked(context.TODO())
if err != nil {
	...
}
```

A lock version number can also be passed using `WithVersion` to determine whether the
lock is held by an owner with the given version number:

```go
locked, err = lock.IsLocked(context.TODO(), atomixlock.WithVersion(version))
if err != nil {
	...
}
```

Once the client has finished with the lock, unlock it by calling `Unlock`:

```go
unlocked, err := lock.Unlock(context.TODO())
if err != nil {
	...
}
```

Clients can also release any process's lock by passing the owner's lock version
number:

```go
unlocked, err = lock.Unlock(context.TODO(), atomixlock.WithVersion(version))
if err != nil {
	...
}
```
