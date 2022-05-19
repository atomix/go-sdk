// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"context"
	primitiveapi "github.com/atomix/api/pkg/atomix"
	"github.com/atomix/go-client/pkg/atomix/primitive/codec"
	"github.com/atomix/go-client/pkg/atomix/util/test"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestListOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	primitiveID := primitiveapi.PrimitiveId{
		Type:      Type.String(),
		Namespace: "test",
		Name:      "TestListOperations",
	}

	test := test.NewRSMTest()
	assert.NoError(t, test.Start())

	conn1, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	conn2, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	list, err := New[string](context.TODO(), "TestListOperations", conn1, WithCodec[string](codec.String()))
	assert.NoError(t, err)
	assert.NotNil(t, list)

	size, err := list.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	_, err = list.Get(context.TODO(), 0)
	assert.Error(t, err)
	assert.True(t, errors.IsInvalid(err))

	err = list.Append(context.TODO(), "foo")
	assert.NoError(t, err)

	size, err = list.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	value, err := list.Get(context.TODO(), 0)
	assert.NoError(t, err)
	assert.Equal(t, "foo", value)

	err = list.Append(context.TODO(), "bar")
	assert.NoError(t, err)

	size, err = list.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 2, size)

	err = list.Insert(context.TODO(), 1, "baz")
	assert.NoError(t, err)

	size, err = list.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 3, size)

	ch := make(chan string)
	err = list.Items(context.TODO(), ch)
	assert.NoError(t, err)

	value, ok := <-ch
	assert.True(t, ok)
	assert.Equal(t, "foo", value)
	value, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "baz", value)
	value, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, "bar", value)

	_, ok = <-ch
	assert.False(t, ok)

	events := make(chan Event[string])
	err = list.Watch(context.TODO(), events)
	assert.NoError(t, err)

	done := make(chan struct{})
	go func() {
		event := <-events
		assert.Equal(t, EventAdd, event.Type)
		assert.Equal(t, 3, event.Index)
		assert.Equal(t, "Hello world!", event.Value)

		event = <-events
		assert.Equal(t, EventAdd, event.Type)
		assert.Equal(t, 2, event.Index)
		assert.Equal(t, "Hello world again!", event.Value)

		event = <-events
		assert.Equal(t, EventRemove, event.Type)
		assert.Equal(t, 1, event.Index)
		assert.Equal(t, "baz", event.Value)

		event = <-events
		assert.Equal(t, EventRemove, event.Type)
		assert.Equal(t, 1, event.Index)
		assert.Equal(t, "Hello world again!", event.Value)

		event = <-events
		assert.Equal(t, EventAdd, event.Type)
		assert.Equal(t, 1, event.Index)
		assert.Equal(t, "Not hello world!", event.Value)

		close(done)
	}()

	err = list.Append(context.TODO(), "Hello world!")
	assert.NoError(t, err)

	err = list.Insert(context.TODO(), 2, "Hello world again!")
	assert.NoError(t, err)

	value, err = list.Remove(context.TODO(), 1)
	assert.NoError(t, err)
	assert.Equal(t, "baz", value)

	err = list.Set(context.TODO(), 1, "Not hello world!")
	assert.NoError(t, err)

	<-done

	err = list.Close(context.Background())
	assert.NoError(t, err)

	list1, err := New[string](context.TODO(), "TestListOperations", conn2, WithCodec[string](codec.String()))
	assert.NoError(t, err)

	size, err = list1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 4, size)

	err = list1.Close(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, test.Stop())
}
