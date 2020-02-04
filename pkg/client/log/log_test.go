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

package log

import (
	"context"
	"testing"
	"time"

	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-client/pkg/client/session"
	"github.com/atomix/go-client/pkg/client/test"
	"github.com/stretchr/testify/assert"
)

func TestLogOperations(t *testing.T) {
	conns, partitions := test.StartTestPartitions(3)

	// Creates a new log primitive
	name := primitive.NewName("default", "test", "default", "test")
	_log, err := New(context.TODO(), name, conns, session.WithTimeout(5*time.Second))
	assert.NoError(t, err)

	// Gets the log entry at index 0
	kv, err := _log.Get(context.Background(), 0)
	assert.NoError(t, err)
	assert.Nil(t, kv)

	// Checks the size of log primitive
	size, err := _log.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	// Appends  an entry to the log
	kv, err = _log.Append(context.Background(), []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	// Appends an entry to the log
	kv, err = _log.Append(context.Background(), []byte("baz"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", string(kv.Value))

	// Gets the first entry in the log
	kv, err = _log.FirstEntry(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(kv.Value))

	// Gets the last entry in the log
	kv, err = _log.LastEntry(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "baz", string(kv.Value))

	// Gets the next entry of the given index in the log
	kv, err = _log.NextEntry(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, "baz", string(kv.Value))

	// Gets the previous entry of the given index in the log
	kv, err = _log.PrevEntry(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(kv.Value))

	// Gets the log entry at index 1
	kv, err = _log.Get(context.Background(), 1)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	// Gets the size of the log primitive
	size, err = _log.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, size)

	// Removes the entry at index 1 from the log
	kv, err = _log.Remove(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(kv.Value))

	// Removes the entry at index 2 from the log
	kv, err = _log.Remove(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, "baz", string(kv.Value))

	// Checks the length of the log primitive
	size, err = _log.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	err = _log.Clear(context.Background())
	assert.NoError(t, err)

	test.StopTestPartitions(partitions)
}

func TestLogStreams(t *testing.T) {
	conns, partitions := test.StartTestPartitions(3)

	name := primitive.NewName("default", "test", "default", "test")
	_log, err := New(context.TODO(), name, conns, session.WithTimeout(5*time.Second))
	assert.NoError(t, err)

	kv, err := _log.Append(context.Background(), []byte("item1"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	c := make(chan *Event)
	latch := make(chan struct{})
	go func() {
		e := <-c
		assert.Equal(t, "item2", string(e.Entry.Value))
		e = <-c
		assert.Equal(t, "item3", string(e.Entry.Value))
		e = <-c
		assert.Equal(t, "item4", string(e.Entry.Value))
		e = <-c
		assert.Equal(t, "item5", string(e.Entry.Value))
		latch <- struct{}{}
	}()

	err = _log.Watch(context.Background(), c)
	assert.NoError(t, err)

	kv, err = _log.Append(context.Background(), []byte("item2"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "item2", string(kv.Value))

	kv, err = _log.Append(context.Background(), []byte("item3"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "item3", string(kv.Value))

	kv, err = _log.Append(context.Background(), []byte("item4"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "item4", string(kv.Value))

	kv, err = _log.Append(context.Background(), []byte("item5"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "item5", string(kv.Value))

	<-latch
	err = _log.Close()
	assert.NoError(t, err)

	log1, err := New(context.TODO(), name, conns, session.WithTimeout(5*time.Second))
	assert.NoError(t, err)

	log2, err := New(context.TODO(), name, conns, session.WithTimeout(5*time.Second))
	assert.NoError(t, err)

	size, err := log1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 5, size)

	err = log1.Close()
	assert.NoError(t, err)

	err = log1.Delete()
	assert.NoError(t, err)

	err = log2.Delete()
	assert.NoError(t, err)

	_log, err = New(context.TODO(), name, conns, session.WithTimeout(5*time.Second))
	assert.NoError(t, err)

	size, err = _log.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	test.StopTestPartitions(partitions)
}
