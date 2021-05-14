// Copyright 2019-present Open Networking Foundation.
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

package leader

import (
	"context"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/test"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLatchOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	primitiveID := primitiveapi.PrimitiveId{
		Type:      Type.String(),
		Namespace: "test",
		Name:      "TestLatchOperations",
	}

	test := test.NewRSMTest()
	assert.NoError(t, test.Start())

	conn1, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	conn2, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	conn3, err := test.CreateProxy(primitiveID)
	assert.NoError(t, err)

	latch1, err := New(context.TODO(), "TestLatchOperations", conn1)
	assert.NoError(t, err)
	assert.NotNil(t, latch1)

	latch2, err := New(context.TODO(), "TestLatchOperations", conn2)
	assert.NoError(t, err)
	assert.NotNil(t, latch2)

	latch3, err := New(context.TODO(), "TestLatchOperations", conn3)
	assert.NoError(t, err)
	assert.NotNil(t, latch3)

	ch := make(chan Event)
	err = latch3.Watch(context.TODO(), ch)
	assert.NoError(t, err)

	term, err := latch1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), term.Revision)
	assert.Equal(t, "", term.Leader)
	assert.Len(t, term.Participants, 0)

	term, err = latch1.Latch(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), term.Revision)
	assert.Equal(t, latch1.ID(), term.Leader)
	assert.Len(t, term.Participants, 1)
	assert.Equal(t, latch1.ID(), term.Participants[0])

	event := <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, uint64(1), event.Leadership.Revision)
	assert.Equal(t, latch1.ID(), event.Leadership.Leader)
	assert.Len(t, event.Leadership.Participants, 1)
	assert.Equal(t, latch1.ID(), event.Leadership.Participants[0])

	term, err = latch2.Latch(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), term.Revision)
	assert.Equal(t, latch1.ID(), term.Leader)
	assert.Len(t, term.Participants, 2)
	assert.Equal(t, latch1.ID(), term.Participants[0])
	assert.Equal(t, latch2.ID(), term.Participants[1])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, uint64(1), event.Leadership.Revision)
	assert.Equal(t, latch1.ID(), event.Leadership.Leader)
	assert.Len(t, event.Leadership.Participants, 2)
	assert.Equal(t, latch1.ID(), event.Leadership.Participants[0])
	assert.Equal(t, latch2.ID(), event.Leadership.Participants[1])

	term, err = latch3.Latch(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), term.Revision)
	assert.Equal(t, latch1.ID(), term.Leader)
	assert.Len(t, term.Participants, 3)
	assert.Equal(t, latch1.ID(), term.Participants[0])
	assert.Equal(t, latch2.ID(), term.Participants[1])
	assert.Equal(t, latch3.ID(), term.Participants[2])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, uint64(1), event.Leadership.Revision)
	assert.Equal(t, latch1.ID(), event.Leadership.Leader)
	assert.Len(t, event.Leadership.Participants, 3)
	assert.Equal(t, latch1.ID(), event.Leadership.Participants[0])
	assert.Equal(t, latch2.ID(), event.Leadership.Participants[1])
	assert.Equal(t, latch3.ID(), event.Leadership.Participants[2])

	err = latch1.Close(context.Background())
	assert.NoError(t, err)

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, uint64(2), event.Leadership.Revision)
	assert.Equal(t, latch2.ID(), event.Leadership.Leader)
	assert.Len(t, event.Leadership.Participants, 2)
	assert.Equal(t, latch2.ID(), event.Leadership.Participants[0])
	assert.Equal(t, latch3.ID(), event.Leadership.Participants[1])

	err = latch2.Close(context.Background())
	assert.NoError(t, err)
	err = latch3.Close(context.Background())
	assert.NoError(t, err)

	latch1, err = New(context.TODO(), "TestLatchOperations", conn1)
	assert.NoError(t, err)

	latch2, err = New(context.TODO(), "TestLatchOperations", conn2)
	assert.NoError(t, err)

	term, err = latch1.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), term.Revision)
	assert.Equal(t, "", term.Leader)
	assert.Len(t, term.Participants, 0)

	err = latch1.Close(context.Background())
	assert.NoError(t, err)

	err = latch1.Delete(context.Background())
	assert.NoError(t, err)

	err = latch2.Delete(context.Background())
	assert.Error(t, err)
	assert.True(t, errors.IsNotFound(err))

	latch, err := New(context.TODO(), "TestLatchOperations", conn3)
	assert.NoError(t, err)

	term, err = latch.Get(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), term.Revision)
	assert.Equal(t, "", term.Leader)
	assert.Len(t, term.Participants, 0)

	assert.NoError(t, test.Stop())
}
