// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"context"
	"github.com/atomix/go-client/pkg/test"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAtomicCounterOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	cluster := test.NewClient()
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	counter1, err := NewBuilder(cluster, "test").Get(ctx)
	assert.NoError(t, err)

	counter2, err := NewBuilder(cluster, "test").Get(ctx)
	assert.NoError(t, err)

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

	value, err = counter2.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(10), value)

	err = counter2.Close(context.Background())
	assert.NoError(t, err)
}
