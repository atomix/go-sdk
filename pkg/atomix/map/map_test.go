// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	primitiveapi "github.com/atomix/api/pkg/atomix"
	"github.com/atomix/go-client/pkg/atomix/primitive/codec"
	"github.com/atomix/go-client/pkg/atomix/util/test"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/meta"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMapOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	primitiveID := primitiveapi.PrimitiveId{
		Type:      Type.String(),
		Namespace: "test",
		Name:      "TestMapOperations",
	}

	test := test.NewRSMTest()
	assert.NoError(t, test.Start())

	conn1, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	_map, err := New[string, string](context.TODO(), "TestMapOperations", conn1, WithCodec[string, string](codec.String(), codec.String()))
	assert.NoError(t, err)

	ch := make(chan Entry[string, string])
	err = _map.Entries(context.Background(), ch)
	assert.NoError(t, err)
	_, ok := <-ch
	assert.False(t, ok)

	kv, err := _map.Get(context.Background(), "foo")
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))
	assert.Nil(t, kv)

	size, err := _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = _map.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	kv1, err := _map.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))

	size, err = _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	kv2, err := _map.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))
	assert.Equal(t, kv1.Revision, kv2.Revision)

	size, err = _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = _map.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	kv, err = _map.Put(context.Background(), "bar", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", string(kv.Value))

	kv, err = _map.Put(context.Background(), "foo", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", string(kv.Value))

	err = _map.Clear(context.Background())
	assert.NoError(t, err)

	size, err = _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = _map.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv1, err = _map.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv2, err = _map.Put(context.Background(), "foo", "baz", IfTimestamp(kv1))
	assert.NoError(t, err)
	assert.NotEqual(t, kv1.Revision, kv2.Revision)
	assert.Equal(t, "baz", string(kv2.Value))

	_, err = _map.Put(context.Background(), "foo", "bar", IfTimestamp(kv1))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	_, err = _map.Remove(context.Background(), "foo", IfTimestamp(meta.ObjectMeta{}))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	removed, err := _map.Remove(context.Background(), "foo", IfTimestamp(kv2))
	assert.NoError(t, err)
	assert.NotNil(t, removed)
	assert.Equal(t, kv2.Revision, removed.Revision)

	assert.NoError(t, test.Stop())
}

func TestMapStreams(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	primitiveID := primitiveapi.PrimitiveId{
		Type:      Type.String(),
		Namespace: "test",
		Name:      "TestMapStreams",
	}

	test := test.NewRSMTest()
	assert.NoError(t, test.Start())

	conn1, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	conn2, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	_map, err := New[string, int](context.TODO(), "TestMapStreams", conn1, WithCodec[string, int](codec.String(), codec.Int()))
	assert.NoError(t, err)

	kv, err := _map.Put(context.Background(), "foo", 1)
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

	err = _map.Watch(context.Background(), c)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	keyCh := make(chan Event[string, int])
	err = _map.Watch(ctx, keyCh, WithFilter(Filter{
		Key: "foo",
	}))
	assert.NoError(t, err)

	kv, err = _map.Put(context.Background(), "foo", 2)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, 2, kv.Value)

	event := <-keyCh
	assert.Equal(t, EventUpdate, event.Type)
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.Equal(t, kv.Revision, event.Entry.Revision)

	kv, err = _map.Put(context.Background(), "bar", 3)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Key)
	assert.Equal(t, 3, kv.Value)

	kv, err = _map.Put(context.Background(), "baz", 4)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", kv.Key)
	assert.Equal(t, 4, kv.Value)

	kv, err = _map.Put(context.Background(), "foo", 5)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, 5, kv.Value)

	event = <-keyCh
	assert.Equal(t, EventUpdate, event.Type)
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.Equal(t, kv.Revision, event.Entry.Revision)

	kv, err = _map.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, 5, kv.Value)

	event = <-keyCh
	assert.Equal(t, EventRemove, event.Type)
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.Equal(t, kv.Revision, event.Entry.Revision)

	cancel()

	_, ok := <-keyCh
	assert.False(t, ok)

	<-latch

	err = _map.Close(context.Background())
	assert.NoError(t, err)

	map1, err := New[string, int](context.TODO(), "TestMapStreams", conn2, WithCodec[string, int](codec.String(), codec.Int()))
	assert.NoError(t, err)

	size, err := map1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 2, size)

	err = map1.Close(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, test.Stop())
}
