// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/test"
	api "github.com/atomix/runtime/api/atomix/runtime/atomic/counter/v1"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAtomicCounterOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	cluster := test.NewCluster()
	defer cluster.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn1, err := cluster.Connect(ctx)
	assert.NoError(t, err)
	client1 := api.NewAtomicCounterClient(conn1)

	conn2, err := cluster.Connect(ctx)
	assert.NoError(t, err)
	client2 := api.NewAtomicCounterClient(conn2)

	counter1, err := New(client1)(ctx, "test")
	assert.NoError(t, err)
	assert.NotNil(t, counter1)

	value, err := counter1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(0), value)

	err = counter1.Set(context.TODO(), 1)
	assert.NoError(t, err)

	value, err = counter1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), value)

	err = counter1.Set(context.TODO(), -1)
	assert.NoError(t, err)

	value, err = counter1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), value)

	value, err = counter1.Increment(context.TODO(), 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), value)

	value, err = counter1.Decrement(context.TODO(), 10)
	assert.NoError(t, err)
	assert.Equal(t, int64(-10), value)

	value, err = counter1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(-10), value)

	value, err = counter1.Increment(context.TODO(), 20)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), value)

	err = counter1.Close(context.Background())
	assert.NoError(t, err)

	counter2, err := New(client2)(ctx, "test")
	assert.NoError(t, err)

	value, err = counter2.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(10), value)

	err = counter2.Close(context.Background())
	assert.NoError(t, err)
}
