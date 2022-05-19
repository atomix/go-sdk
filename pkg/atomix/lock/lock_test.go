// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	primitiveapi "github.com/atomix/api/pkg/atomix/v1"
	"github.com/atomix/go-client/pkg/atomix/util/test"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	primitiveID := primitiveapi.PrimitiveId{
		Type:      Type.String(),
		Namespace: "test",
		Name:      "TestLock",
	}

	test := test.NewRSMTest()
	assert.NoError(t, test.Start())

	conn1, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	conn2, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	l1, err := New(context.TODO(), "TestLock", conn1)
	assert.NoError(t, err)
	l2, err := New(context.TODO(), "TestLock", conn2)
	assert.NoError(t, err)

	v1, err := l1.Lock(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, 0, v1)

	locked, err := l1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, StateLocked, locked.State)

	locked, err = l2.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, StateLocked, locked.State)

	var v2 Status
	c := make(chan struct{})
	go func() {
		_, err := l2.Lock(context.Background())
		assert.NoError(t, err)
		c <- struct{}{}
	}()

	err = l1.Unlock(context.Background())
	assert.NoError(t, err)

	<-c

	assert.NotEqual(t, v1, v2)

	locked, err = l1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, StateLocked, locked.State)

	//locked, err = l1.Get(context.Background(), IfMatch(v1))
	//assert.NoError(t, err)
	//assert.Equal(t, StateUnlocked, locked.State)

	locked, err = l1.Get(context.Background(), IfMatch(v2))
	assert.NoError(t, err)
	assert.Equal(t, StateLocked, locked.State)

	v2, err = l2.Lock(context.Background(), WithTimeout(1*time.Second))
	assert.Error(t, err)
	assert.True(t, errors.IsTimeout(err))
	assert.Equal(t, StateLocked, v2.State)

	err = l1.Close(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, test.Stop())
}
