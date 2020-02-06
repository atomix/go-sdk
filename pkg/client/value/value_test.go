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

package value

import (
	"context"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-client/pkg/client/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValue(t *testing.T) {
	partitions, closers := test.StartTestPartitions(3)
	defer test.StopTestPartitions(closers)

	sessions, err := test.OpenSessions(partitions)
	assert.NoError(t, err)
	defer test.CloseSessions(sessions)

	name := primitive.NewName("default", "test", "default", "test")
	value, err := New(context.TODO(), name, sessions)
	assert.NoError(t, err)
	assert.NotNil(t, value)

	val, version, err := value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Nil(t, val)
	assert.Equal(t, uint64(0), version)

	ch := make(chan *Event)

	err = value.Watch(context.TODO(), ch)
	assert.NoError(t, err)

	_, err = value.Set(context.TODO(), []byte("foo"), IfVersion(1))
	assert.EqualError(t, err, "version mismatch")

	_, err = value.Set(context.TODO(), []byte("foo"), IfValue([]byte("bar")))
	assert.EqualError(t, err, "value mismatch")

	version, err = value.Set(context.TODO(), []byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), version)

	val, version, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), version)
	assert.Equal(t, "foo", string(val))

	_, err = value.Set(context.TODO(), []byte("foo"), IfVersion(2))
	assert.EqualError(t, err, "version mismatch")

	version, err = value.Set(context.TODO(), []byte("bar"), IfVersion(1))
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), version)

	val, version, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), version)
	assert.Equal(t, "bar", string(val))

	version, err = value.Set(context.TODO(), []byte("baz"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), version)

	val, version, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), version)
	assert.Equal(t, "baz", string(val))

	event := <-ch
	assert.Equal(t, EventUpdated, event.Type)
	assert.Equal(t, uint64(1), event.Version)
	assert.Equal(t, "foo", string(event.Value))

	event = <-ch
	assert.Equal(t, EventUpdated, event.Type)
	assert.Equal(t, uint64(2), event.Version)
	assert.Equal(t, "bar", string(event.Value))

	event = <-ch
	assert.Equal(t, EventUpdated, event.Type)
	assert.Equal(t, uint64(3), event.Version)
	assert.Equal(t, "baz", string(event.Value))

	err = value.Close(context.Background())
	assert.NoError(t, err)

	value1, err := New(context.TODO(), name, sessions)
	assert.NoError(t, err)

	value2, err := New(context.TODO(), name, sessions)
	assert.NoError(t, err)

	val, _, err = value1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, "baz", string(val))

	err = value1.Close(context.Background())
	assert.NoError(t, err)

	err = value1.Delete(context.Background())
	assert.NoError(t, err)

	err = value2.Delete(context.Background())
	assert.NoError(t, err)

	value, err = New(context.TODO(), name, sessions)
	assert.NoError(t, err)

	val, _, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Nil(t, val)
}
