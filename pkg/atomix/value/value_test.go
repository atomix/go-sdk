// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"context"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"github.com/atomix/go-client/pkg/atomix/primitive/codec"
	"github.com/atomix/go-client/pkg/atomix/util/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValueOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	primitiveID := primitiveapi.PrimitiveId{
		Type:      Type.String(),
		Namespace: "test",
		Name:      "TestRSMValue",
	}

	test := test.NewRSMTest()
	assert.NoError(t, test.Start())

	conn1, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	conn2, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	value, err := New[string](context.TODO(), "TestRSMValue", conn1, WithCodec[string](codec.String()))
	assert.NoError(t, err)
	assert.NotNil(t, value)

	val, md, err := value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Nil(t, val)
	assert.Equal(t, meta.Revision(0), md.Revision)

	ch := make(chan Event[string])

	err = value.Watch(context.TODO(), ch)
	assert.NoError(t, err)

	_, err = value.Set(context.TODO(), "foo", IfMatch(meta.ObjectMeta{Revision: 1}))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	md, err = value.Set(context.TODO(), "foo")
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(1), md.Revision)

	val, md, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(1), md.Revision)
	assert.Equal(t, "foo", val)

	_, err = value.Set(context.TODO(), "foo", IfMatch(meta.ObjectMeta{Revision: 2}))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	md, err = value.Set(context.TODO(), "bar", IfMatch(meta.ObjectMeta{Revision: 1}))
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(2), md.Revision)

	val, md, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(2), md.Revision)
	assert.Equal(t, "bar", val)

	md, err = value.Set(context.TODO(), "baz")
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(3), md.Revision)

	val, md, err = value.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(3), md.Revision)
	assert.Equal(t, "baz", val)

	event := <-ch
	assert.Equal(t, EventUpdate, event.Type)
	assert.Equal(t, meta.Revision(1), event.Revision)
	assert.Equal(t, "foo", event.Value)

	event = <-ch
	assert.Equal(t, EventUpdate, event.Type)
	assert.Equal(t, meta.Revision(2), event.Revision)
	assert.Equal(t, "bar", event.Value)

	event = <-ch
	assert.Equal(t, EventUpdate, event.Type)
	assert.Equal(t, meta.Revision(3), event.Revision)
	assert.Equal(t, "baz", event.Value)

	err = value.Close(context.Background())
	assert.NoError(t, err)

	value1, err := New[string](context.TODO(), "TestRSMValue", conn2, WithCodec[string](codec.String()))
	assert.NoError(t, err)

	val, _, err = value1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, "baz", val)

	err = value1.Close(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, test.Stop())
}
