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
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/util/test"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
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

	list, err := New(context.TODO(), "TestListOperations", conn1)
	assert.NoError(t, err)
	assert.NotNil(t, list)

	size, err := list.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	_, err = list.Get(context.TODO(), 0)
	assert.Error(t, err)
	assert.True(t, errors.IsInvalid(err))

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

	events := make(chan Event)
	err = list.Watch(context.TODO(), events)
	assert.NoError(t, err)

	done := make(chan struct{})
	go func() {
		event := <-events
		assert.Equal(t, EventAdd, event.Type)
		assert.Equal(t, 3, event.Index)
		assert.Equal(t, "Hello world!", string(event.Value))

		event = <-events
		assert.Equal(t, EventAdd, event.Type)
		assert.Equal(t, 2, event.Index)
		assert.Equal(t, "Hello world again!", string(event.Value))

		event = <-events
		assert.Equal(t, EventRemove, event.Type)
		assert.Equal(t, 1, event.Index)
		assert.Equal(t, "baz", string(event.Value))

		event = <-events
		assert.Equal(t, EventRemove, event.Type)
		assert.Equal(t, 1, event.Index)
		assert.Equal(t, "Hello world again!", string(event.Value))

		event = <-events
		assert.Equal(t, EventAdd, event.Type)
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

	list1, err := New(context.TODO(), "TestListOperations", conn2)
	assert.NoError(t, err)

	size, err = list1.Len(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, 4, size)

	err = list1.Close(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, test.Stop())
}
