// Copyright 2020-present Open Networking Foundation.
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

package rsm

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/atomix/test"
	"github.com/atomix/atomix-sdk-go/pkg/logging"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRSMTest(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	test := test.NewTest(NewProtocol(), test.WithPartitions(1), test.WithReplicas(1))
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

	kv, err := map1.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))

	kv, err = map2.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))

	err = map1.Close(context.TODO())
	assert.NoError(t, err)

	err = map2.Close(context.TODO())
	assert.NoError(t, err)
}
