// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"context"
	"fmt"
	"github.com/atomix/go-sdk/pkg/generic"
	"github.com/atomix/go-sdk/pkg/test"
	"github.com/atomix/runtime/sdk/pkg/async"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestMapEntries(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	client := test.NewMemoryClient()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	map1, err := NewBuilder[string, string](client, "test").
		Codec(generic.Scalar[string]()).
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

func TestHugeEntry(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	client := test.NewConsensusClient(3, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m, err := NewBuilder[string, string](client, "test").
		Codec(generic.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	n := 1024 * 1024 * 5
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(rand.Int())
	}
	s := string(b)

	_, err = m.Put(ctx, "foo", s)
	assert.NoError(t, err)

	e, err := m.Get(ctx, "foo")
	assert.NoError(t, err)
	assert.Equal(t, "foo", e.Key)
	assert.Equal(t, s, e.Value)
}

func TestMapOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	client := test.NewMemoryClient()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	map1, err := NewBuilder[string, string](client, "test").
		Codec(generic.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	map2, err := NewBuilder[string, string](client, "test").
		Codec(generic.Scalar[string]()).
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

	kv, err = map2.Put(context.Background(), "bar", "baz")
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

func TestMapPutSerial(t *testing.T) {
	client := test.NewConsensusClient(3, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m, err := NewBuilder[string, string](client, "test").
		Codec(generic.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	iterations := 1000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		_, err := m.Put(ctx, "foo", "bar")
		if err != nil {
			t.Fail()
		}
	}
	end := time.Now()
	println(fmt.Sprintf("completed %d writes in %s", iterations, end.Sub(start)))
}

func TestMapPutConcurrent(t *testing.T) {
	client := test.NewConsensusClient(3, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m, err := NewBuilder[string, string](client, "test").
		Codec(generic.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	numKeys := 1000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = uuid.New().String()
	}

	count := &atomic.Uint64{}
	concurrency := 10000
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				_, err := m.Put(ctx, keys[rand.Intn(numKeys)], "bar")
				if err != nil {
					t.Fail()
				}
				count.Add(1)
			}
		}()
	}

	ticker := time.NewTicker(10 * time.Second)
	for {
		<-ticker.C
		println(fmt.Sprintf("completed %d writes in 10 seconds", count.Swap(0)))
	}
}

func TestMapGetSerial(t *testing.T) {
	client := test.NewConsensusClient(3, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m, err := NewBuilder[string, string](client, "test").
		Codec(generic.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	_, err = m.Put(ctx, "foo", "bar")
	if err != nil {
		t.Fail()
	}

	iterations := 1000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		_, err := m.Get(ctx, "foo")
		if err != nil {
			t.Fail()
		}
	}
	end := time.Now()
	println(fmt.Sprintf("completed %d reads in %s", iterations, end.Sub(start)))
}

func TestMapGetConcurrent(t *testing.T) {
	client := test.NewConsensusClient(3, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m, err := NewBuilder[string, string](client, "test").
		Codec(generic.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	numKeys := 1000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = uuid.New().String()
	}

	err = async.IterAsync(numKeys, func(i int) error {
		_, err := m.Put(ctx, keys[i], uuid.New().String())
		return err
	})
	if err != nil {
		t.Fail()
	}

	count := &atomic.Uint64{}
	concurrency := 1000
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				_, err := m.Get(ctx, keys[rand.Intn(numKeys)])
				if err != nil {
					t.Fail()
				}
				count.Add(1)
			}
		}()
	}

	ticker := time.NewTicker(10 * time.Second)
	for {
		<-ticker.C
		println(fmt.Sprintf("completed %d reads in 10 seconds", count.Swap(0)))
	}
}

func TestMapPutGetConcurrent(t *testing.T) {
	client := test.NewConsensusClient(3, 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m, err := NewBuilder[string, string](client, "test").
		Codec(generic.Scalar[string]()).
		Get(ctx)
	assert.NoError(t, err)

	numKeys := 1000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = uuid.New().String()
	}

	err = async.IterAsync(numKeys, func(i int) error {
		_, err := m.Put(ctx, keys[i], uuid.New().String())
		return err
	})
	if err != nil {
		t.Fail()
	}

	putCount := &atomic.Uint64{}
	putConcurrency := 100
	for i := 0; i < putConcurrency; i++ {
		go func() {
			for {
				_, err := m.Put(ctx, keys[rand.Intn(numKeys)], keys[rand.Intn(numKeys)])
				if err != nil {
					t.Fail()
				}
				putCount.Add(1)
			}
		}()
	}

	getCount := &atomic.Uint64{}
	getConcurrency := 100
	for i := 0; i < getConcurrency; i++ {
		go func() {
			for {
				_, err := m.Get(ctx, keys[rand.Intn(numKeys)])
				if err != nil {
					t.Fail()
				}
				getCount.Add(1)
			}
		}()
	}

	ticker := time.NewTicker(10 * time.Second)
	for {
		<-ticker.C
		println(fmt.Sprintf("completed %d puts in 10 seconds", putCount.Swap(0)))
		println(fmt.Sprintf("completed %d gets in 10 seconds", getCount.Swap(0)))
	}
}
