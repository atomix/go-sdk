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
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver/v1"
	"github.com/atomix/atomix-go-client/pkg/atomix/util/test"
	"github.com/atomix/atomix-sdk-go/pkg/errors"
	"github.com/atomix/atomix-sdk-go/pkg/logging"
	"github.com/atomix/atomix-sdk-go/pkg/meta"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValueOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	primitiveID := driverapi.ProxyId{
		Type: Type.String(),
		Name: "TestRSMValue",
	}

	test := test.NewRSMTest()
	assert.NoError(t, test.Start())

	conn1, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	conn2, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	value, err := New(context.TODO(), "TestRSMValue", conn1)
	assert.NoError(t, err)
	assert.NotNil(t, value)

	val, md, err := value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Nil(t, val)
	assert.Equal(t, meta.Revision(0), md.Revision)

	ch := make(chan Event)

	err = value.Watch(context.TODO(), ch)
	assert.NoError(t, err)

	_, err = value.Set(context.TODO(), []byte("foo"), IfMatch(meta.ObjectMeta{Revision: 1}))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	md, err = value.Set(context.TODO(), []byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(1), md.Revision)

	val, md, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(1), md.Revision)
	assert.Equal(t, "foo", string(val))

	_, err = value.Set(context.TODO(), []byte("foo"), IfMatch(meta.ObjectMeta{Revision: 2}))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	md, err = value.Set(context.TODO(), []byte("bar"), IfMatch(meta.ObjectMeta{Revision: 1}))
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(2), md.Revision)

	val, md, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(2), md.Revision)
	assert.Equal(t, "bar", string(val))

	md, err = value.Set(context.TODO(), []byte("baz"))
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(3), md.Revision)

	val, md, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(3), md.Revision)
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

	value1, err := New(context.TODO(), "TestRSMValue", conn2)
	assert.NoError(t, err)

	val, _, err = value1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, "baz", string(val))

	err = value1.Close(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, test.Stop())
}
