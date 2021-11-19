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

package indexedmap

import (
	"context"
	"github.com/atomix/atomix-sdk-go/pkg/errors"
	"github.com/atomix/atomix-sdk-go/pkg/logging"
	"github.com/atomix/atomix-sdk-go/pkg/meta"
	"github.com/atomix/atomix-sdk-go/pkg/test"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexedMapOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	test := test.New()
	assert.NoError(t, test.Start())

	conn1, err := test.Connect()
	assert.NoError(t, err)

	_map, err := New(context.TODO(), "TestIndexedMapOperations", conn1)
	assert.NoError(t, err)

	kv, err := _map.Get(context.Background(), "foo")
	assert.Error(t, err)
	assert.Nil(t, kv)
	assert.True(t, errors.IsNotFound(err))

	size, err := _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = _map.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(1), kv.Index)
	assert.Equal(t, "bar", string(kv.Value))

	kv, err = _map.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(1), kv.Index)
	assert.Equal(t, "bar", string(kv.Value))

	kv, err = _map.GetIndex(context.Background(), 1)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(1), kv.Index)
	assert.Equal(t, "bar", string(kv.Value))
	assert.NotEqual(t, meta.Revision(0), kv.Revision)
	version := kv.Revision

	size, err = _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	kv, err = _map.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, Index(1), kv.Index)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))
	assert.NotEqual(t, meta.Revision(0), kv.Revision)
	assert.Equal(t, version, kv.Revision)

	size, err = _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = _map.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(2), kv.Index)
	assert.Equal(t, "bar", string(kv.Value))

	kv1, err := _map.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv2, err := _map.Set(context.Background(), 2, "foo", []byte("baz"), IfMatch(kv1))
	assert.NoError(t, err)
	assert.NotEqual(t, kv1.Revision, kv2.Revision)
	assert.Equal(t, "baz", string(kv2.Value))

	_, err = _map.Set(context.Background(), 2, "foo", []byte("bar"), IfMatch(kv1))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	_, err = _map.Remove(context.Background(), "foo", IfMatch(meta.ObjectMeta{}))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	removed, err := _map.Remove(context.Background(), "foo", IfMatch(kv2))
	assert.NoError(t, err)
	assert.NotNil(t, removed)
	assert.Equal(t, kv2.Revision, removed.Revision)

	kv, err = _map.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(3), kv.Index)
	assert.Equal(t, "bar", string(kv.Value))

	kv, err = _map.Put(context.Background(), "bar", []byte("baz"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Key)
	assert.Equal(t, Index(4), kv.Index)
	assert.Equal(t, "baz", string(kv.Value))

	kv, err = _map.Put(context.Background(), "foo", []byte("baz"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(3), kv.Index)
	assert.Equal(t, "baz", string(kv.Value))

	index, err := _map.FirstIndex(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, Index(3), index)

	index, err = _map.LastIndex(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, Index(4), index)

	index, err = _map.PrevIndex(context.Background(), Index(4))
	assert.NoError(t, err)
	assert.Equal(t, Index(3), index)

	index, err = _map.NextIndex(context.Background(), Index(2))
	assert.NoError(t, err)
	assert.Equal(t, Index(3), index)

	kv, err = _map.FirstEntry(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, Index(3), kv.Index)

	kv, err = _map.LastEntry(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, Index(4), kv.Index)

	kv, err = _map.PrevEntry(context.Background(), Index(4))
	assert.NoError(t, err)
	assert.Equal(t, Index(3), kv.Index)

	kv, err = _map.NextEntry(context.Background(), Index(3))
	assert.NoError(t, err)
	assert.Equal(t, Index(4), kv.Index)

	kv, err = _map.PrevEntry(context.Background(), Index(3))
	assert.Error(t, err)
	assert.Nil(t, kv)
	assert.True(t, errors.IsNotFound(err))

	kv, err = _map.NextEntry(context.Background(), Index(4))
	assert.Error(t, err)
	assert.Nil(t, kv)
	assert.True(t, errors.IsNotFound(err))

	kv, err = _map.RemoveIndex(context.Background(), 4)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, Index(4), kv.Index)
	assert.Equal(t, "bar", kv.Key)
	assert.Equal(t, "baz", string(kv.Value))

	err = _map.Clear(context.Background())
	assert.NoError(t, err)

	size, err = _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = _map.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	assert.NoError(t, test.Stop())
}

func TestIndexedMapStreams(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	test := test.New()
	assert.NoError(t, test.Start())

	conn1, err := test.Connect()
	assert.NoError(t, err)

	conn2, err := test.Connect()
	assert.NoError(t, err)

	_map, err := New(context.TODO(), "TestIndexedMapStreams", conn1)
	assert.NoError(t, err)

	kv, err := _map.Put(context.Background(), "foo", []byte{1})
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	c := make(chan Event)
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

	keyCh := make(chan Event)
	err = _map.Watch(context.Background(), keyCh, WithFilter(Filter{
		Key: "foo",
	}))
	assert.NoError(t, err)

	kv, err = _map.Put(context.Background(), "foo", []byte{2})
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, byte(2), kv.Value[0])
	assert.NotEqual(t, meta.Revision(0), kv.Revision)

	event := <-keyCh
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.NotEqual(t, meta.Revision(0), event.Entry.Revision)
	assert.Equal(t, kv.Revision, event.Entry.Revision)

	indexCh := make(chan Event)
	err = _map.Watch(context.Background(), indexCh, WithFilter(Filter{
		Index: kv.Index,
	}))
	assert.NoError(t, err)

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
	assert.NotEqual(t, meta.Revision(0), event.Entry.Revision)
	assert.Equal(t, kv.Revision, event.Entry.Revision)

	event = <-indexCh
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.NotEqual(t, meta.Revision(0), event.Entry.Revision)
	assert.Equal(t, kv.Revision, event.Entry.Revision)

	chanEntry := make(chan Entry)
	go func() {
		e, ok := <-chanEntry
		assert.True(t, ok)
		assert.Equal(t, "foo", e.Key)
		e, ok = <-chanEntry
		assert.True(t, ok)
		assert.Equal(t, "bar", e.Key)
		e, ok = <-chanEntry
		assert.True(t, ok)
		assert.Equal(t, "baz", e.Key)
		latch <- struct{}{}
	}()

	<-latch

	err = _map.Entries(context.Background(), chanEntry)
	assert.NoError(t, err)

	<-latch

	err = _map.Close(context.Background())
	assert.NoError(t, err)

	map1, err := New(context.TODO(), "TestIndexedMapStreams", conn2)
	assert.NoError(t, err)

	size, err := map1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 3, size)

	err = map1.Close(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, test.Stop())
}
