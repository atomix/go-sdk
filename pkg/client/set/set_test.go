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

package set

import (
	"context"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-client/pkg/client/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetOperations(t *testing.T) {
	partitions, closers := test.StartTestPartitions(3)
	defer test.StopTestPartitions(closers)

	sessions, err := test.OpenSessions(partitions)
	assert.NoError(t, err)
	defer test.CloseSessions(sessions)

	name := primitive.NewName("default", "test", "default", "test")
	set, err := New(context.TODO(), name, sessions)
	assert.NoError(t, err)
	assert.NotNil(t, set)

	size, err := set.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	contains, err := set.Contains(context.TODO(), "foo")
	assert.NoError(t, err)
	assert.False(t, contains)

	added, err := set.Add(context.TODO(), "foo")
	assert.NoError(t, err)
	assert.True(t, added)

	size, err = set.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	contains, err = set.Contains(context.TODO(), "foo")
	assert.NoError(t, err)
	assert.True(t, contains)

	added, err = set.Add(context.TODO(), "bar")
	assert.NoError(t, err)
	assert.True(t, added)

	added, err = set.Add(context.TODO(), "foo")
	assert.NoError(t, err)
	assert.False(t, added)

	size, err = set.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 2, size)

	added, err = set.Add(context.TODO(), "baz")
	assert.NoError(t, err)
	assert.True(t, added)

	size, err = set.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 3, size)

	ch := make(chan string)
	err = set.Elements(context.TODO(), ch)
	assert.NoError(t, err)

	value, ok := <-ch
	assert.True(t, ok)
	assert.Contains(t, []string{"foo", "bar", "baz"}, value)
	value, ok = <-ch
	assert.True(t, ok)
	assert.Contains(t, []string{"foo", "bar", "baz"}, value)
	value, ok = <-ch
	assert.True(t, ok)
	assert.Contains(t, []string{"foo", "bar", "baz"}, value)

	_, ok = <-ch
	assert.False(t, ok)

	events := make(chan *Event)
	err = set.Watch(context.TODO(), events, WithReplay())
	assert.NoError(t, err)

	done := make(chan bool)
	go func() {
		event := <-events
		assert.Equal(t, EventNone, event.Type)
		assert.Contains(t, []string{"foo", "bar", "baz"}, event.Value)

		event = <-events
		assert.Equal(t, EventNone, event.Type)
		assert.Contains(t, []string{"foo", "bar", "baz"}, event.Value)

		event = <-events
		assert.Equal(t, EventNone, event.Type)
		assert.Contains(t, []string{"foo", "bar", "baz"}, event.Value)

		done <- true

		event = <-events
		assert.Equal(t, EventAdded, event.Type)
		assert.Equal(t, "Hello world!", event.Value)

		event = <-events
		assert.Equal(t, EventAdded, event.Type)
		assert.Equal(t, "Hello world again!", event.Value)

		event = <-events
		assert.Equal(t, EventRemoved, event.Type)
		assert.Equal(t, "baz", event.Value)

		close(done)
	}()

	<-done

	added, err = set.Add(context.TODO(), "Hello world!")
	assert.NoError(t, err)
	assert.True(t, added)

	added, err = set.Add(context.TODO(), "Hello world again!")
	assert.NoError(t, err)
	assert.True(t, added)

	removed, err := set.Remove(context.TODO(), "baz")
	assert.NoError(t, err)
	assert.True(t, removed)

	removed, err = set.Remove(context.TODO(), "baz")
	assert.NoError(t, err)
	assert.False(t, removed)

	<-done

	err = set.Close(context.Background())
	assert.NoError(t, err)

	set1, err := New(context.TODO(), name, sessions)
	assert.NoError(t, err)

	set2, err := New(context.TODO(), name, sessions)
	assert.NoError(t, err)

	size, err = set1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 4, size)

	err = set1.Close(context.Background())
	assert.NoError(t, err)

	err = set1.Delete(context.Background())
	assert.NoError(t, err)

	err = set2.Delete(context.Background())
	assert.NoError(t, err)

	set, err = New(context.TODO(), name, sessions)
	assert.NoError(t, err)

	size, err = set.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)
}
