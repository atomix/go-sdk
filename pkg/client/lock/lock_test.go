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
	"github.com/atomix/go-client/pkg/client/test/rsm"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	lockrsm "github.com/atomix/go-framework/pkg/atomix/protocol/rsm/lock"
	lockproxy "github.com/atomix/go-framework/pkg/atomix/proxy/rsm/lock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	test := rsm.NewTest().
		SetPartitions(1).
		SetSessions(3).
		SetStorage(lockrsm.RegisterService).
		SetProxy(lockproxy.RegisterProxy)

	conns, err := test.Start()
	assert.NoError(t, err)
	defer test.Stop()

	l1, err := New(context.TODO(), "test", conns[0])
	assert.NoError(t, err)
	l2, err := New(context.TODO(), "test", conns[1])
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

	l, err := New(context.TODO(), "test", conns[2])
	assert.NoError(t, err)

	locked, err = l.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, StateUnlocked, locked.State)
}
