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
	"github.com/atomix/go-client/pkg/client/meta"
	"github.com/atomix/go-client/pkg/client/test"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	valuersm "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/value"
	valueproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/value"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValue(t *testing.T) {
	test := test.New().
		SetPartitions(1).
		SetSessions(3).
		SetStorage(valuersm.RegisterService).
		SetProxy(valueproxy.RegisterProxy)

	conns, err := test.Start()
	assert.NoError(t, err)
	defer test.Stop()

	value, err := New(context.TODO(), "test", conns[0])
	assert.NoError(t, err)
	assert.NotNil(t, value)

	val, version, err := value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Nil(t, val)
	assert.Equal(t, uint64(0), version)

	ch := make(chan Event)

	err = value.Watch(context.TODO(), ch)
	assert.NoError(t, err)

	_, err = value.Set(context.TODO(), []byte("foo"), IfMatch(meta.ObjectMeta{Revision: 1}))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	version, err = value.Set(context.TODO(), []byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), version)

	val, version, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), version)
	assert.Equal(t, "foo", string(val))

	_, err = value.Set(context.TODO(), []byte("foo"), IfMatch(meta.ObjectMeta{Revision: 2}))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	version, err = value.Set(context.TODO(), []byte("bar"), IfMatch(meta.ObjectMeta{Revision: 1}))
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
	assert.Equal(t, EventUpdate, event.Type)
	assert.Equal(t, meta.Revision(1), event.Revision)
	assert.Equal(t, "foo", string(event.Value))

	event = <-ch
	assert.Equal(t, EventUpdate, event.Type)
	assert.Equal(t, meta.Revision(2), event.Revision)
	assert.Equal(t, "bar", string(event.Value))

	event = <-ch
	assert.Equal(t, EventUpdate, event.Type)
	assert.Equal(t, meta.Revision(3), event.Revision)
	assert.Equal(t, "baz", string(event.Value))

	err = value.Close(context.Background())
	assert.NoError(t, err)

	value1, err := New(context.TODO(), "test", conns[1])
	assert.NoError(t, err)

	value2, err := New(context.TODO(), "test", conns[2])
	assert.NoError(t, err)

	val, _, err = value1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, "baz", string(val))

	err = value1.Close(context.Background())
	assert.NoError(t, err)

	err = value1.Delete(context.Background())
	assert.NoError(t, err)

	err = value2.Delete(context.Background())
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))

	value, err = New(context.TODO(), "test", conns[0])
	assert.NoError(t, err)

	val, _, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Nil(t, val)
}
