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
	"github.com/atomix/go-client/pkg/atomix/test/rsm"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	setrsm "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/set"
	setproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/set"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetOperations(t *testing.T) {
	test := rsm.NewTest().
		SetPartitions(1).
		SetSessions(3).
		SetStorage(setrsm.RegisterService).
		SetProxy(setproxy.RegisterProxy)

	conns, err := test.Start()
	assert.NoError(t, err)
	defer test.Stop()

	set, err := New(context.TODO(), "test", conns[0])
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

	events := make(chan Event)
	err = set.Watch(context.TODO(), events, WithReplay())
	assert.NoError(t, err)

	done := make(chan bool)
	go func() {
		event := <-events
		assert.Equal(t, EventReplay, event.Type)
		assert.Contains(t, []string{"foo", "bar", "baz"}, event.Value)

		event = <-events
		assert.Equal(t, EventReplay, event.Type)
		assert.Contains(t, []string{"foo", "bar", "baz"}, event.Value)

		event = <-events
		assert.Equal(t, EventReplay, event.Type)
		assert.Contains(t, []string{"foo", "bar", "baz"}, event.Value)

		done <- true

		event = <-events
		assert.Equal(t, EventAdd, event.Type)
		assert.Equal(t, "Hello world!", event.Value)

		event = <-events
		assert.Equal(t, EventAdd, event.Type)
		assert.Equal(t, "Hello world again!", event.Value)

		event = <-events
		assert.Equal(t, EventRemove, event.Type)
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

	set1, err := New(context.TODO(), "test", conns[1])
	assert.NoError(t, err)

	set2, err := New(context.TODO(), "test", conns[2])
	assert.NoError(t, err)

	size, err = set1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 4, size)

	err = set1.Close(context.Background())
	assert.NoError(t, err)

	err = set1.Delete(context.Background())
	assert.NoError(t, err)

	err = set2.Delete(context.Background())
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))

	set, err = New(context.TODO(), "test", conns[0])
	assert.NoError(t, err)

	size, err = set.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)
}
