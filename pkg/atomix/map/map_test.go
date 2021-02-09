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
	"github.com/atomix/go-client/pkg/atomix/test/gossip"
	"github.com/atomix/go-client/pkg/atomix/test/rsm"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	gossipmap "github.com/atomix/go-framework/pkg/atomix/protocol/gossip/map"
	maprsm "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/map"
	gossipproxy "github.com/atomix/go-framework/pkg/atomix/proxy/gossip/map"
	mapproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/map"
	atime "github.com/atomix/go-framework/pkg/atomix/time"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMapOperations(t *testing.T) {
	test := rsm.NewTest().
		SetPartitions(1).
		SetSessions(3).
		SetStorage(maprsm.RegisterService).
		SetProxy(mapproxy.RegisterProxy)

	conns, err := test.Start()
	assert.NoError(t, err)
	defer test.Stop()

	_map, err := New(context.TODO(), "test", conns[0])
	assert.NoError(t, err)

	kv, err := _map.Get(context.Background(), "foo")
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))
	assert.Nil(t, kv)

	size, err := _map.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = _map.Put(context.Background(), "foo", []byte("bar"))
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

	kv1, err = _map.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	_, err = _map.Put(context.Background(), "foo", []byte("baz"), IfMatch(kv1))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	kv2, err = _map.Put(context.Background(), "foo", []byte("baz"), IfMatch(kv1))
	assert.NoError(t, err)
	assert.NotEqual(t, kv1.Revision, kv2.Revision)
	assert.Equal(t, "baz", string(kv2.Value))

	_, err = _map.Remove(context.Background(), "foo", IfMatch(meta.ObjectMeta{}))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	removed, err := _map.Remove(context.Background(), "foo", IfMatch(kv2))
	assert.NoError(t, err)
	assert.NotNil(t, removed)
	assert.Equal(t, kv2.Revision, removed.Revision)
}

func TestMapStreams(t *testing.T) {
	test := rsm.NewTest().
		SetPartitions(1).
		SetSessions(3).
		SetStorage(maprsm.RegisterService).
		SetProxy(mapproxy.RegisterProxy)

	conns, err := test.Start()
	assert.NoError(t, err)
	defer test.Stop()

	_map, err := New(context.TODO(), "test", conns[0])
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

	event := <-keyCh
	assert.NotNil(t, event)
	assert.Equal(t, "foo", event.Entry.Key)
	assert.True(t, kv.Timestamp.Equal(event.Entry.Timestamp))

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
	assert.True(t, kv.Timestamp.Equal(event.Entry.Timestamp))

	<-latch

	err = _map.Close(context.Background())
	assert.NoError(t, err)

	map1, err := New(context.TODO(), "test", conns[1])
	assert.NoError(t, err)

	map2, err := New(context.TODO(), "test", conns[2])
	assert.NoError(t, err)

	size, err := map1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 3, size)

	err = map1.Close(context.Background())
	assert.NoError(t, err)

	err = map1.Delete(context.Background())
	assert.NoError(t, err)

	err = map2.Delete(context.Background())
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))

	_map, err = New(context.TODO(), "test", conns[0])
	assert.NoError(t, err)

	size, err = _map.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)
}

func TestGossipMap(t *testing.T) {
	test := gossip.NewTest().
		SetScheme(atime.LogicalScheme).
		SetReplicas(3).
		SetPartitions(3).
		SetClients(3).
		SetServer(gossipmap.RegisterServer).
		SetStorage(gossipmap.RegisterService).
		SetProxy(gossipproxy.RegisterProxy)

	conns, err := test.Start()
	assert.NoError(t, err)
	defer test.Stop()

	map1, err := New(context.TODO(), "test", conns[0])
	assert.NoError(t, err)
	assert.NotNil(t, map1)

	map2, err := New(context.TODO(), "test", conns[1])
	assert.NoError(t, err)
	assert.NotNil(t, map2)

	entry, err := map1.Get(context.TODO(), "foo")
	assert.True(t, errors.IsNotFound(err))
	assert.Nil(t, entry)

	entry, err = map2.Get(context.TODO(), "bar")
	assert.True(t, errors.IsNotFound(err))
	assert.Nil(t, entry)

	entry, err = map1.Put(context.TODO(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "bar", string(entry.Value))

	time.Sleep(time.Second)

	entry, err = map2.Get(context.TODO(), "foo")
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "bar", string(entry.Value))
}
