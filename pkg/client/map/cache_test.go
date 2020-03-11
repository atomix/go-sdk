// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package _map //nolint:golint

import (
	"context"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-client/pkg/client/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCachedMapOperations(t *testing.T) {
	partitions, closers := test.StartTestPartitions(3)
	defer test.StopTestPartitions(closers)

	sessions, err := test.OpenSessions(partitions)
	assert.NoError(t, err)
	defer test.CloseSessions(sessions)

	name := primitive.NewName("default", "test", "default", "test")
	_map, err := New(context.TODO(), name, sessions, WithCache(1))
	assert.NoError(t, err)

	kv, err := _map.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.Nil(t, kv)

	size, err := _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = _map.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	kv, err = _map.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))
	version := kv.Version

	size, err = _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	kv, err = _map.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))
	assert.Equal(t, version, kv.Version)

	size, err = _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = _map.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	kv, err = _map.Put(context.Background(), "bar", []byte("baz"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", string(kv.Value))

	kv, err = _map.Put(context.Background(), "foo", []byte("baz"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", string(kv.Value))

	err = _map.Clear(context.Background())
	assert.NoError(t, err)

	size, err = _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = _map.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv1, err := _map.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	_, err = _map.Put(context.Background(), "foo", []byte("baz"), IfVersion(1))
	assert.Error(t, err)

	kv2, err := _map.Put(context.Background(), "foo", []byte("baz"), IfVersion(kv1.Version))
	assert.NoError(t, err)
	assert.NotEqual(t, kv1.Version, kv2.Version)
	assert.Equal(t, "baz", string(kv2.Value))

	_, err = _map.Remove(context.Background(), "foo", IfVersion(1))
	assert.Error(t, err)

	removed, err := _map.Remove(context.Background(), "foo", IfVersion(kv2.Version))
	assert.NoError(t, err)
	assert.NotNil(t, removed)
	assert.Equal(t, kv2.Version, removed.Version)
}

func TestCachedMapStreams(t *testing.T) {
	partitions, closers := test.StartTestPartitions(3)
	defer test.StopTestPartitions(closers)

	sessions, err := test.OpenSessions(partitions)
	assert.NoError(t, err)
	defer test.CloseSessions(sessions)

	name := primitive.NewName("default", "test", "default", "test")
	_map, err := New(context.TODO(), name, sessions, WithCache(1))
	assert.NoError(t, err)

	kv, err := _map.Put(context.Background(), "foo", []byte{1})
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	c := make(chan *Event)
	latch := make(chan struct{})
	go func() {
		e := <-c
		assert.Equal(t, "foo", e.Entry.Key)
		assert.Equal(t, byte(2), e.Entry.Value[0])
		e = <-c
		assert.Equal(t, "bar", e.Entry.Key)
		assert.Equal(t, byte(3), e.Entry.Value[0])
		e = <-c
		assert.Equal(t, "baz", e.Entry.Key)
		assert.Equal(t, byte(4), e.Entry.Value[0])
		e = <-c
		assert.Equal(t, "foo", e.Entry.Key)
		assert.Equal(t, byte(5), e.Entry.Value[0])
		latch <- struct{}{}
	}()

	err = _map.Watch(context.Background(), c)
	assert.NoError(t, err)

	keyCh := make(chan *Event)
	err = _map.Watch(context.Background(), keyCh, WithFilter(Filter{
		Key: "foo",
	}))
	assert.NoError(t, err)

	kv, err = _map.Put(context.Background(), "foo", []byte{2})
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, byte(2), kv.Value[0])

	event := <-keyCh
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.Equal(t, kv.Version, event.Entry.Version)

	kv, err = _map.Put(context.Background(), "bar", []byte{3})
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Key)
	assert.Equal(t, byte(3), kv.Value[0])

	kv, err = _map.Put(context.Background(), "baz", []byte{4})
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", kv.Key)
	assert.Equal(t, byte(4), kv.Value[0])

	kv, err = _map.Put(context.Background(), "foo", []byte{5})
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, byte(5), kv.Value[0])

	event = <-keyCh
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.Equal(t, kv.Version, event.Entry.Version)

	<-latch

	err = _map.Close(context.Background())
	assert.NoError(t, err)

	map1, err := New(context.TODO(), name, sessions, WithCache(1))
	assert.NoError(t, err)

	map2, err := New(context.TODO(), name, sessions, WithCache(1))
	assert.NoError(t, err)

	size, err := map1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 3, size)

	err = map1.Close(context.Background())
	assert.NoError(t, err)

	err = map1.Delete(context.Background())
	assert.NoError(t, err)

	err = map2.Delete(context.Background())
	assert.NoError(t, err)

	_map, err = New(context.TODO(), name, sessions, WithCache(1))
	assert.NoError(t, err)

	size, err = _map.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)
}
