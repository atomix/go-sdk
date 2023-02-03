// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"context"
	"encoding/json"
	"github.com/atomix/atomix/api/errors"
	indexedmapv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/test"
	"github.com/atomix/go-sdk/pkg/types"
	gogotypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

func TestIndexedMapOperations(t *testing.T) {
	testIndexedMapOperations(t, runtimev1.RoutingRule{Names: []string{"*"}})
}

func TestCachingIndexedMapOperations(t *testing.T) {
	config := indexedmapv1.Config{
		Cache: indexedmapv1.CacheConfig{
			Enabled: true,
			Size_:   3,
		},
	}
	bytes, err := json.Marshal(config)
	assert.NoError(t, err)
	testIndexedMapOperations(t, runtimev1.RoutingRule{
		Names: []string{"*"},
		Config: &gogotypes.Any{
			Value: bytes,
		},
	})
}

func TestMirroredIndexedMapOperations(t *testing.T) {
	config := indexedmapv1.Config{
		Cache: indexedmapv1.CacheConfig{
			Enabled: true,
		},
	}
	bytes, err := json.Marshal(config)
	assert.NoError(t, err)
	testIndexedMapOperations(t, runtimev1.RoutingRule{
		Names: []string{"*"},
		Config: &gogotypes.Any{
			Value: bytes,
		},
	})
}

func testIndexedMapOperations(t *testing.T, rule runtimev1.RoutingRule) {
	logging.SetLevel(logging.DebugLevel)

	cluster := test.NewClient(rule)
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	map1, err := NewBuilder[string, string](cluster, "test").
		Codec(types.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	kv, err := map1.Get(context.Background(), "foo")
	assert.Error(t, err)
	assert.Nil(t, kv)
	assert.True(t, errors.IsNotFound(err))

	size, err := map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map1.Append(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(1), kv.Index)
	assert.Equal(t, "bar", kv.Value)

	kv, err = map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(1), kv.Index)
	assert.Equal(t, "bar", kv.Value)

	kv, err = map1.GetIndex(context.Background(), 1)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(1), kv.Index)
	assert.Equal(t, "bar", kv.Value)
	assert.NotEqual(t, primitive.Version(0), kv.Version)
	version := kv.Version

	size, err = map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	kv, err = map1.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, Index(1), kv.Index)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", kv.Value)
	assert.NotEqual(t, primitive.Version(0), kv.Version)
	assert.Equal(t, version, kv.Version)

	size, err = map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map1.Append(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(2), kv.Index)
	assert.Equal(t, "bar", kv.Value)

	kv1, err := map1.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv2, err := map1.Update(context.Background(), "foo", "baz", IfVersion(kv1.Version))
	assert.NoError(t, err)
	assert.NotEqual(t, kv1.Version, kv2.Version)
	assert.Equal(t, "baz", string(kv2.Value))

	_, err = map1.Update(context.Background(), "foo", "bar", IfVersion(kv1.Version))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	_, err = map1.Remove(context.Background(), "foo", IfVersion(math.MaxInt))
	assert.Error(t, err)
	assert.True(t, errors.IsConflict(err))

	removed, err := map1.Remove(context.Background(), "foo", IfVersion(kv2.Version))
	assert.NoError(t, err)
	assert.NotNil(t, removed)
	assert.Equal(t, kv2.Version, removed.Version)

	kv, err = map1.Append(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(3), kv.Index)
	assert.Equal(t, "bar", kv.Value)

	kv, err = map1.Append(context.Background(), "bar", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Key)
	assert.Equal(t, Index(4), kv.Index)
	assert.Equal(t, "baz", kv.Value)

	kv, err = map1.Update(context.Background(), "foo", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, Index(3), kv.Index)
	assert.Equal(t, "baz", kv.Value)

	index, err := map1.FirstIndex(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, Index(3), index)

	index, err = map1.LastIndex(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, Index(4), index)

	index, err = map1.PrevIndex(context.Background(), Index(4))
	assert.NoError(t, err)
	assert.Equal(t, Index(3), index)

	index, err = map1.NextIndex(context.Background(), Index(2))
	assert.NoError(t, err)
	assert.Equal(t, Index(3), index)

	kv, err = map1.FirstEntry(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, Index(3), kv.Index)

	kv, err = map1.LastEntry(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, Index(4), kv.Index)

	kv, err = map1.PrevEntry(context.Background(), Index(4))
	assert.NoError(t, err)
	assert.Equal(t, Index(3), kv.Index)

	kv, err = map1.NextEntry(context.Background(), Index(3))
	assert.NoError(t, err)
	assert.Equal(t, Index(4), kv.Index)

	kv, err = map1.PrevEntry(context.Background(), Index(3))
	assert.Error(t, err)
	assert.Nil(t, kv)
	assert.True(t, errors.IsNotFound(err))

	kv, err = map1.NextEntry(context.Background(), Index(4))
	assert.Error(t, err)
	assert.Nil(t, kv)
	assert.True(t, errors.IsNotFound(err))

	kv, err = map1.RemoveIndex(context.Background(), 4)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, Index(4), kv.Index)
	assert.Equal(t, "bar", kv.Key)
	assert.Equal(t, "baz", kv.Value)

	err = map1.Clear(context.Background())
	assert.NoError(t, err)

	size, err = map1.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = map1.Append(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
}

func TestIndexedMapStreams(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	cluster := test.NewClient()
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	map1, err := NewBuilder[string, string](cluster, "test").
		Codec(types.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	events, err := map1.Events(context.Background())
	assert.NoError(t, err)

	kv, err := map1.Append(context.Background(), "foo", "bar")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	event, err := events.Next()
	assert.NoError(t, err)
	insert, ok := event.(*Inserted[string, string])
	assert.True(t, ok)
	assert.Equal(t, "foo", insert.Entry.Key)
	assert.Equal(t, "bar", insert.Entry.Value)
	assert.Equal(t, kv.Version, insert.Entry.Version)

	filteredEvents, err := map1.Events(context.Background(), WithFilter(Filter{
		Key: "foo",
	}))
	assert.NoError(t, err)

	kv, err = map1.Update(context.Background(), "foo", "baz")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "baz", kv.Value)
	assert.NotEqual(t, primitive.Version(0), kv.Version)

	event, err = events.Next()
	assert.NoError(t, err)
	update, ok := event.(*Updated[string, string])
	assert.True(t, ok)
	assert.Equal(t, "foo", update.Entry.Key)
	assert.Equal(t, "baz", update.Entry.Value)
	assert.Equal(t, kv.Version, update.Entry.Version)

	event, err = filteredEvents.Next()
	assert.NoError(t, err)
	update, ok = event.(*Updated[string, string])
	assert.True(t, ok)
	assert.Equal(t, "foo", update.Entry.Key)
	assert.Equal(t, "baz", update.Entry.Value)
	assert.Equal(t, kv.Version, update.Entry.Version)
}
