// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	"github.com/atomix/go-client/pkg/generic"
	"github.com/atomix/go-client/pkg/test"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMapOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	cluster := test.NewCluster()
	defer cluster.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	map1, err := NewBuilder[string, string](cluster, "test").
		Codec(generic.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	map2, err := NewBuilder[string, string](cluster, "test").
		Codec(generic.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	kv, err := map1.Get(context.Background(), "foo")
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))
	assert.Nil(t, kv)

	size, err := map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map1.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Value)

	kv1, err := map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", kv.Value)

	size, err = map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	kv2, err := map1.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv2)
	assert.Equal(t, "foo", kv2.Key)
	assert.Equal(t, "bar", kv2.Value)
	assert.Equal(t, kv1.Version, kv2.Version)

	size, err = map2.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map2.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Value)

	kv, err = map2.Put(context.Background(), "bar", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", kv.Value)

	kv, err = map2.Put(context.Background(), "foo", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", kv.Value)

	err = map2.Clear(context.Background())
	assert.NoError(t, err)

	size, err = map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map1.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv1, err = map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv2, err = map1.Update(context.Background(), "foo", "baz", IfVersion(kv1.Version))
	assert.NoError(t, err)
	assert.NotEqual(t, kv1.Version, kv2.Version)
	assert.Equal(t, "baz", kv2.Value)

	_, err = map1.Update(context.Background(), "foo", "bar", IfVersion(kv1.Version))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	_, err = map1.Remove(context.Background(), "foo", IfVersion(kv1.Version))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	removed, err := map1.Remove(context.Background(), "foo", IfVersion(kv2.Version))
	assert.NoError(t, err)
	assert.NotNil(t, removed)
	assert.Equal(t, kv2.Version, removed.Version)
}
