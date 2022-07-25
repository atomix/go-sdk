// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/test"
	api "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	mapv1 "github.com/atomix/runtime/sdk/primitives/map/v1"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMapOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	test := test.New(mapv1.Type)
	defer test.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn1, err := test.Connect(ctx)
	assert.NoError(t, err)
	client1 := api.NewMapClient(conn1)

	conn2, err := test.Connect(ctx)
	assert.NoError(t, err)
	client2 := api.NewMapClient(conn2)

	map1, err := New[string, string](client1)(ctx, "test",
		WithKeyType[string, string](generic.String()),
		WithValueType[string, string](generic.String()))
	assert.NoError(t, err)

	map2, err := New[string, string](client2)(ctx, "test",
		WithKeyType[string, string](generic.String()),
		WithValueType[string, string](generic.String()))
	assert.NoError(t, err)

	ch := make(chan Entry[string, string])
	err = map1.Entries(context.Background(), ch)
	assert.NoError(t, err)
	_, ok := <-ch
	assert.False(t, ok)

	kv, err := map1.Get(context.Background(), "foo")
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))
	assert.Nil(t, kv)

	size, err := map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map1.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Value)

	kv1, err := map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", kv.Value)

	size, err = map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	kv2, err := map1.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", kv.Value)
	assert.Equal(t, kv1.Timestamp, kv2.Timestamp)

	size, err = map2.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map2.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Value)

	kv, err = map2.Put(context.Background(), "bar", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", kv.Value)

	kv, err = map2.Put(context.Background(), "foo", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", kv.Value)

	err = map2.Clear(context.Background())
	assert.NoError(t, err)

	size, err = map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map1.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv1, err = map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv2, err = map1.Update(context.Background(), "foo", "baz", IfTimestamp(kv1.Timestamp))
	assert.NoError(t, err)
	assert.NotEqual(t, kv1.Timestamp, kv2.Timestamp)
	assert.Equal(t, "baz", kv2.Value)

	_, err = map1.Update(context.Background(), "foo", "bar", IfTimestamp(kv1.Timestamp))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	_, err = map1.Remove(context.Background(), "foo", IfTimestamp(kv1.Timestamp))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	removed, err := map1.Remove(context.Background(), "foo", IfTimestamp(kv2.Timestamp))
	assert.NoError(t, err)
	assert.NotNil(t, removed)
	assert.Equal(t, kv2.Timestamp, removed.Timestamp)
}

func TestMapStreams(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	test := test.New(mapv1.Type)
	defer test.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn1, err := test.Connect(ctx)
	assert.NoError(t, err)
	client1 := api.NewMapClient(conn1)

	conn2, err := test.Connect(ctx)
	assert.NoError(t, err)
	client2 := api.NewMapClient(conn2)

	map1, err := New[string, int](client1)(ctx, "test",
		WithKeyType[string, int](generic.String()),
		WithValueType[string, int](generic.Int()))
	assert.NoError(t, err)

	map2, err := New[string, int](client2)(ctx, "test",
		WithKeyType[string, int](generic.String()),
		WithValueType[string, int](generic.Int()))
	assert.NoError(t, err)

	kv, err := map1.Put(context.Background(), "foo", 1)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	c := make(chan Event[string, int])
	latch := make(chan struct{})
	go func() {
		e := <-c
		assert.Equal(t, "foo", e.Entry.Key)
		assert.Equal(t, 2, e.Entry.Value)
		e = <-c
		assert.Equal(t, "bar", e.Entry.Key)
		assert.Equal(t, 3, e.Entry.Value)
		e = <-c
		assert.Equal(t, "baz", e.Entry.Key)
		assert.Equal(t, 4, e.Entry.Value)
		e = <-c
		assert.Equal(t, "foo", e.Entry.Key)
		assert.Equal(t, 5, e.Entry.Value)
		latch <- struct{}{}
	}()

	err = map1.Watch(context.Background(), c)
	assert.NoError(t, err)

	ctx, cancel = context.WithCancel(context.Background())
	keyCh := make(chan Event[string, int])
	err = map1.Watch(ctx, keyCh, WithFilter(Filter{
		Key: "foo",
	}))
	assert.NoError(t, err)

	kv, err = map1.Put(context.Background(), "foo", 2)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, 2, kv.Value)

	event := <-keyCh
	assert.Equal(t, EventUpdate, event.Type)
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.Equal(t, kv.Timestamp, event.Entry.Timestamp)

	kv, err = map1.Put(context.Background(), "bar", 3)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Key)
	assert.Equal(t, 3, kv.Value)

	kv, err = map1.Put(context.Background(), "baz", 4)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", kv.Key)
	assert.Equal(t, 4, kv.Value)

	kv, err = map1.Put(context.Background(), "foo", 5)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, 5, kv.Value)

	event = <-keyCh
	assert.Equal(t, EventUpdate, event.Type)
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.Equal(t, kv.Timestamp, event.Entry.Timestamp)

	kv, err = map1.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, 5, kv.Value)

	event = <-keyCh
	assert.Equal(t, EventRemove, event.Type)
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.Equal(t, kv.Timestamp, event.Entry.Timestamp)

	cancel()

	_, ok := <-keyCh
	assert.False(t, ok)

	<-latch

	err = map1.Close(context.Background())
	assert.NoError(t, err)

	size, err := map2.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 2, size)

	err = map2.Close(context.Background())
	assert.NoError(t, err)
}
