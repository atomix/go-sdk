// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/test"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGossipTest(t *testing.T) {
	t.Skip()

	logging.SetLevel(logging.DebugLevel)

	test := test.NewTest(
		NewProtocol(),
		test.WithPartitions(3),
		test.WithReplicas(3),
		test.WithDebugLogs())
	assert.NoError(t, test.Start())
	defer test.Stop()

	client1, err := test.NewClient("test-1")
	assert.NoError(t, err)

	client2, err := test.NewClient("test-2")
	assert.NoError(t, err)

	map1, err := client1.GetMap(context.TODO(), "test")
	assert.NoError(t, err)

	map2, err := client2.GetMap(context.TODO(), "test")
	assert.NoError(t, err)

	kv, err := map1.Put(context.Background(), "a", []byte("b"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "a", kv.Key)
	assert.Equal(t, "b", string(kv.Value))

	time.Sleep(time.Second)

	kv, err = map2.Get(context.Background(), "a")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "a", kv.Key)
	assert.Equal(t, "b", string(kv.Value))

	time.Sleep(time.Second)

	kv, err = map1.Put(context.Background(), "b", []byte("c"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "b", kv.Key)
	assert.Equal(t, "c", string(kv.Value))

	time.Sleep(time.Second)

	kv, err = map2.Put(context.Background(), "c", []byte("d"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "c", kv.Key)
	assert.Equal(t, "d", string(kv.Value))

	time.Sleep(time.Second)

	kv, err = map1.Put(context.Background(), "d", []byte("e"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "d", kv.Key)
	assert.Equal(t, "e", string(kv.Value))

	time.Sleep(time.Second)

	kv, err = map2.Put(context.Background(), "e", []byte("f"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "e", kv.Key)
	assert.Equal(t, "f", string(kv.Value))

	err = map1.Close(context.TODO())
	assert.NoError(t, err)

	err = map2.Close(context.TODO())
	assert.NoError(t, err)
}
