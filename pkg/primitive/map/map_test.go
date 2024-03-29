// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	"encoding/json"
	"github.com/atomix/atomix/api/errors"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/test"
	"github.com/atomix/go-sdk/pkg/types"
	gogotypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

func TestMapEntries(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	cluster := test.NewClient()
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	map1, err := NewBuilder[string, string](cluster, "test").
		Codec(types.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	_, err = map1.Put(ctx, "foo", "bar")
	assert.NoError(t, err)
	_, err = map1.Put(ctx, "bar", "baz")
	assert.NoError(t, err)
	_, err = map1.Put(ctx, "baz", "foo")
	assert.NoError(t, err)

	stream, err := map1.List(ctx)
	assert.NoError(t, err)

	for {
		entry, err := stream.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		assert.NotNil(t, entry)
	}
}

func TestMapTransactions(t *testing.T) {
	testMapTransactions(t, runtimev1.RoutingRule{Names: []string{"*"}})
}

func TestCachingMapTransactions(t *testing.T) {
	config := mapv1.Config{
		Cache: mapv1.CacheConfig{
			Enabled: true,
			Size_:   3,
		},
	}
	bytes, err := json.Marshal(config)
	assert.NoError(t, err)
	testMapTransactions(t, runtimev1.RoutingRule{
		Names: []string{"*"},
		Config: &gogotypes.Any{
			Value: bytes,
		},
	})
}

func TestMirroredMapTransactions(t *testing.T) {
	config := mapv1.Config{
		Cache: mapv1.CacheConfig{
			Enabled: true,
		},
	}
	bytes, err := json.Marshal(config)
	assert.NoError(t, err)
	testMapTransactions(t, runtimev1.RoutingRule{
		Names: []string{"*"},
		Config: &gogotypes.Any{
			Value: bytes,
		},
	})
}

func testMapTransactions(t *testing.T, rule runtimev1.RoutingRule) {
	logging.SetLevel(logging.DebugLevel)

	client := test.NewClient(rule)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	map1, err := NewBuilder[string, string](client, "test").
		Codec(types.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	map2, err := NewBuilder[string, string](client, "test").
		Codec(types.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	kv, err := map1.Get(context.Background(), "foo")
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))
	assert.Nil(t, kv)

	_, err = map1.Transaction(context.Background()).
		Put("foo", "bar").
		Insert("bar", "baz").
		Update("baz", "bar").
		Commit()
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))

	entries, err := map1.Transaction(context.Background()).
		Put("foo", "bar").
		Insert("bar", "baz").
		Insert("baz", "foo").
		Commit()
	assert.NoError(t, err)
	assert.Len(t, entries, 3)
	assert.Equal(t, "foo", entries[0].Key)
	assert.Equal(t, "bar", entries[0].Value)
	assert.Equal(t, "bar", entries[1].Key)
	assert.Equal(t, "baz", entries[1].Value)
	assert.Equal(t, "baz", entries[2].Key)
	assert.Equal(t, "foo", entries[2].Value)

	kv, err = map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", kv.Value)

	kv, err = map2.Get(context.Background(), "bar")
	assert.NoError(t, err)
	assert.Equal(t, "baz", kv.Value)
}

func TestMapOperations(t *testing.T) {
	testMapOperations(t, runtimev1.RoutingRule{Names: []string{"*"}})
}

func TestCachingMapOperations(t *testing.T) {
	config := mapv1.Config{
		Cache: mapv1.CacheConfig{
			Enabled: true,
			Size_:   3,
		},
	}
	bytes, err := json.Marshal(config)
	assert.NoError(t, err)
	testMapOperations(t, runtimev1.RoutingRule{
		Names: []string{"*"},
		Config: &gogotypes.Any{
			Value: bytes,
		},
	})
}

func TestMirroredMapOperations(t *testing.T) {
	config := mapv1.Config{
		Cache: mapv1.CacheConfig{
			Enabled: true,
		},
	}
	bytes, err := json.Marshal(config)
	assert.NoError(t, err)
	testMapOperations(t, runtimev1.RoutingRule{
		Names: []string{"*"},
		Config: &gogotypes.Any{
			Value: bytes,
		},
	})
}

func testMapOperations(t *testing.T, rule runtimev1.RoutingRule) {
	logging.SetLevel(logging.DebugLevel)

	client := test.NewClient(rule)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	map1, err := NewBuilder[string, string](client, "test").
		Codec(types.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	map2, err := NewBuilder[string, string](client, "test").
		Codec(types.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	stream, err := map1.Watch(streamCtx)
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

	value, err := stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, value)

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

	value, err = stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, value)

	size, err = map2.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map2.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Value)

	value, err = stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, value)

	kv, err = map2.Insert(context.Background(), "foo", "baz")
	assert.Error(t, err)
	assert.True(t, errors.IsAlreadyExists(err))
	assert.Nil(t, kv)

	kv, err = map2.Insert(context.Background(), "bar", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", kv.Value)

	value, err = stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, value)

	kv, err = map2.Put(context.Background(), "foo", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", kv.Value)

	value, err = stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, value)

	entries, err := map1.List(context.Background())
	for {
		entry, err := entries.Next()
		if err == io.EOF {
			break
		}
		assert.NotNil(t, entry)
	}

	err = map2.Clear(context.Background())
	assert.NoError(t, err)

	value, err = stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, value)

	value, err = stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, value)

	size, err = map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map1.Put(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	value, err = stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, value)

	kv1, err = map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv2, err = map1.Update(context.Background(), "foo", "baz", IfVersion(kv1.Version))
	assert.NoError(t, err)
	assert.NotEqual(t, kv1.Version, kv2.Version)
	assert.Equal(t, "baz", kv2.Value)

	value, err = stream.Next()
	assert.NoError(t, err)
	assert.NotNil(t, value)

	streamCancel()

	value, err = stream.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, value)

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
