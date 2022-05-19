// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	"context"
	primitiveapi "github.com/atomix/api/pkg/atomix"
	"github.com/atomix/go-client/pkg/atomix/primitive/codec"
	"github.com/atomix/go-client/pkg/atomix/util/test"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	primitiveID := primitiveapi.PrimitiveId{
		Type:      Type.String(),
		Namespace: "test",
		Name:      "TestSetOperations",
	}

	test := test.NewRSMTest()
	assert.NoError(t, test.Start())

	conn1, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	conn2, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	set, err := New[string](context.TODO(), "TestSetOperations", conn1, WithCodec[string](codec.String()))
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

	events := make(chan Event[string])
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

	set1, err := New(context.TODO(), "TestSetOperations", conn2)
	assert.NoError(t, err)

	size, err = set1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 4, size)

	err = set1.Close(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, test.Stop())
}
