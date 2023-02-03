// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/test"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	cluster := test.NewClient()
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	l1, err := NewBuilder(cluster, "test").Get(ctx)
	assert.NoError(t, err)
	l2, err := NewBuilder(cluster, "test").Get(ctx)
	assert.NoError(t, err)

	version, err := l1.Get(context.Background())
	assert.Error(t, err)
	assert.Equal(t, Version(0), version)

	v1, err := l1.Lock(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, Version(0), v1)

	version, err = l1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, v1, version)

	version, err = l2.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, v1, version)

	c := make(chan Version)
	go func() {
		version, err := l2.Lock(context.Background())
		assert.NoError(t, err)
		c <- version
	}()

	err = l1.Unlock(context.Background())
	assert.NoError(t, err)

	v2 := <-c

	assert.NotEqual(t, v1, v2)

	version, err = l1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, v2, version)

	version, err = l1.Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, v2, version)

	v2, err = l2.Lock(context.Background(), WithTimeout(1*time.Second))
	assert.Error(t, err)
	assert.Equal(t, Version(0), v2)

	err = l1.Close(context.Background())
	assert.NoError(t, err)
}
