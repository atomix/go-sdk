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

package list

import (
	"context"
	"github.com/atomix/go-client/pkg/client/database/primitive"
	"github.com/atomix/go-client/pkg/client/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestListOperations(t *testing.T) {
	partitions, closers := test.StartTestPartitions(3)
	defer test.StopTestPartitions(closers)

	sessions, err := test.OpenSessions(partitions)
	assert.NoError(t, err)
	defer test.CloseSessions(sessions)

	name := primitive.NewName("default", "test", "default", "test")
	list, err := New(context.TODO(), name, sessions)
	assert.NoError(t, err)
	assert.NotNil(t, list)

	size, err := list.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	_, err = list.Get(context.TODO(), 0)
	assert.EqualError(t, err, "index out of bounds")

	err = list.Append(context.TODO(), []byte("foo"))
	assert.NoError(t, err)

	size, err = list.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	value, err := list.Get(context.TODO(), 0)
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(value))

	err = list.Append(context.TODO(), []byte("bar"))
	assert.NoError(t, err)

	size, err = list.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 2, size)

	err = list.Insert(context.TODO(), 1, []byte("baz"))
	assert.NoError(t, err)

	size, err = list.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 3, size)

	ch := make(chan []byte)
	err = list.Items(context.TODO(), ch)
	assert.NoError(t, err)

	value, ok := <-ch
	assert.True(t, ok)
	assert.Equal(t, "foo", string(value))
	value, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "baz", string(value))
	value, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "bar", string(value))

	_, ok = <-ch
	assert.False(t, ok)

	events := make(chan *Event)
	err = list.Watch(context.TODO(), events)
	assert.NoError(t, err)

	done := make(chan struct{})
	go func() {
		event := <-events
		assert.Equal(t, EventInserted, event.Type)
		assert.Equal(t, 3, event.Index)
		assert.Equal(t, "Hello world!", string(event.Value))

		event = <-events
		assert.Equal(t, EventInserted, event.Type)
		assert.Equal(t, 2, event.Index)
		assert.Equal(t, "Hello world again!", string(event.Value))

		event = <-events
		assert.Equal(t, EventRemoved, event.Type)
		assert.Equal(t, 1, event.Index)
		assert.Equal(t, "baz", string(event.Value))

		event = <-events
		assert.Equal(t, EventRemoved, event.Type)
		assert.Equal(t, 1, event.Index)
		assert.Equal(t, "Hello world again!", string(event.Value))

		event = <-events
		assert.Equal(t, EventInserted, event.Type)
		assert.Equal(t, 1, event.Index)
		assert.Equal(t, "Not hello world!", string(event.Value))

		close(done)
	}()

	err = list.Append(context.TODO(), []byte("Hello world!"))
	assert.NoError(t, err)

	err = list.Insert(context.TODO(), 2, []byte("Hello world again!"))
	assert.NoError(t, err)

	value, err = list.Remove(context.TODO(), 1)
	assert.NoError(t, err)
	assert.Equal(t, "baz", string(value))

	err = list.Set(context.TODO(), 1, []byte("Not hello world!"))
	assert.NoError(t, err)

	<-done

	err = list.Close(context.Background())
	assert.NoError(t, err)

	list1, err := New(context.TODO(), name, sessions)
	assert.NoError(t, err)

	list2, err := New(context.TODO(), name, sessions)
	assert.NoError(t, err)

	size, err = list1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 4, size)

	err = list1.Close(context.Background())
	assert.NoError(t, err)

	err = list1.Delete(context.Background())
	assert.NoError(t, err)

	err = list2.Delete(context.Background())
	assert.NoError(t, err)

	list, err = New(context.TODO(), name, sessions)
	assert.NoError(t, err)

	size, err = list.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	assert.NoError(t, list.Append(context.TODO(), []byte("1")))
	assert.NoError(t, list.Append(context.TODO(), []byte("2")))
	assert.NoError(t, list.Append(context.TODO(), []byte("3")))
	assert.NoError(t, list.Append(context.TODO(), []byte("4")))
	assert.NoError(t, list.Append(context.TODO(), []byte("5")))
	assert.NoError(t, list.Append(context.TODO(), []byte("6")))

	slice, err := list.Slice(context.TODO(), 1, 4)
	assert.NoError(t, err)
	size, err = slice.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 3, size)
	value, err = slice.Get(context.TODO(), 0)
	assert.NoError(t, err)
	assert.Equal(t, "2", string(value))
	value, err = slice.Get(context.TODO(), 2)
	assert.NoError(t, err)
	assert.Equal(t, "4", string(value))

	ch = make(chan []byte)
	err = slice.Items(context.TODO(), ch)
	assert.NoError(t, err)

	assert.Equal(t, "2", string(<-ch))
	assert.Equal(t, "3", string(<-ch))
	assert.Equal(t, "4", string(<-ch))

	_, ok = <-ch
	assert.False(t, ok)

	slice, err = list.SliceFrom(context.TODO(), 1)
	assert.NoError(t, err)
	size, err = slice.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 5, size)
	value, err = slice.Get(context.TODO(), 0)
	assert.NoError(t, err)
	assert.Equal(t, "2", string(value))
	value, err = slice.Get(context.TODO(), 2)
	assert.NoError(t, err)
	assert.Equal(t, "4", string(value))

	ch = make(chan []byte)
	err = slice.Items(context.TODO(), ch)
	assert.NoError(t, err)

	assert.Equal(t, "2", string(<-ch))
	assert.Equal(t, "3", string(<-ch))
	assert.Equal(t, "4", string(<-ch))
	assert.Equal(t, "5", string(<-ch))
	assert.Equal(t, "6", string(<-ch))

	_, ok = <-ch
	assert.False(t, ok)
}
