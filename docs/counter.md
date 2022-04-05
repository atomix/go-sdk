<!--
SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
SPDX-License-Identifier: Apache-2.0
-->

# Counter

The `Counter` primitive is a distributed counter that supports atomic increment, decrement,
and check-and-set operations. To create a counter, call `GetCounter` on the database in which
to create the counter:

```go
myCounter, err := atomix.GetCounter(context.Background(), "my-counter")
if err != nil {
	...
}

defer myCounter.Close(context.Background())
```

```go
count, err := myCounter.Get(context.Background())
if err != nil {
	...
}
```

```go
count, err = myCounter.Set(context.Background(), 10)
if err != nil {
	...
}
```

```go
count, err = myCounter.Increment(context.Background(), 1)
if err != nil {
	...
}
```

```go
count, err = myCounter.Decrement(context.Background(), 10)
if err !=  nil {
	...
}
```
