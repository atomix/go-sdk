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

package lock

import (
	"context"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/util/test"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
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

	conn3, err := test.CreateProxy(primitiveID)
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

	locked, err = l1.Get(context.Background(), IfMatch(v1))
	assert.NoError(t, err)
	assert.Equal(t, StateUnlocked, locked.State)

	locked, err = l1.Get(context.Background(), IfMatch(v2))
	assert.NoError(t, err)
	assert.Equal(t, StateLocked, locked.State)

	v2, err = l2.Lock(context.Background(), WithTimeout(1*time.Second))
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), v2)

	err = l1.Close(context.Background())
	assert.NoError(t, err)

	err = l1.Delete(context.Background())
	assert.NoError(t, err)

	err = l2.Delete(context.Background())
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))

	l, err := New(context.TODO(), "TestLock", conn3)
	assert.NoError(t, err)

	locked, err = l.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, StateUnlocked, locked.State)

	assert.NoError(t, test.Stop())
}
