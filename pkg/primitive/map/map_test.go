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
	"io"
	"testing"
	"time"
)

func TestMapOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	cluster := test.NewClient()
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

	streamCtx, streamCancel := context.WithCancel(context.Background())
	stream, err := map1.Watch(streamCtx)
	assert.NoError(t, err)

	value, err := map1.Get(context.Background(), "foo")
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))
	assert.Equal(t, "", value)

	size, err := map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	value, err = map1.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, "", value)

	entry, err := stream.Next()
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "bar", entry.Value)

	value, err = map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, "bar", value)

	size, err = map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	value, err = map1.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, "bar", value)

	entry, err = stream.Next()
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "", entry.Value)

	size, err = map2.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	value, err = map2.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, "", value)

	entry, err = stream.Next()
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "bar", entry.Value)

	value, err = map2.Put(context.Background(), "bar", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, "", value)

	entry, err = stream.Next()
	assert.NoError(t, err)
	assert.Equal(t, "bar", entry.Key)
	assert.Equal(t, "baz", entry.Value)

	value, err = map2.Put(context.Background(), "foo", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, "bar", value)

	entry, err = stream.Next()
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "baz", entry.Value)

	err = map2.Clear(context.Background())
	assert.NoError(t, err)

	entry, err = stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, entry)

	entry, err = stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, entry)

	size, err = map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	value, err = map1.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, value)

	entry, err = stream.Next()
	assert.NoError(t, err)
	assert.Equal(t, "foo", entry.Key)
	assert.Equal(t, "bar", entry.Value)

	value, err = map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, value)

	streamCancel()

	entry, err = stream.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, entry)
}
