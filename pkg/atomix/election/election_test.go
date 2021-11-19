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

package election

import (
	"context"
	"github.com/atomix/atomix-sdk-go/pkg/logging"
	"github.com/atomix/atomix-sdk-go/pkg/meta"
	"github.com/atomix/atomix-sdk-go/pkg/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestElectionOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	test := test.New()
	assert.NoError(t, test.Start())

	conn1, err := test.Connect()
	assert.NoError(t, err)

	conn2, err := test.Connect()
	assert.NoError(t, err)

	conn3, err := test.Connect()
	assert.NoError(t, err)

	election1, err := New(context.TODO(), "TestElectionOperations", conn1)
	assert.NoError(t, err)
	assert.NotNil(t, election1)

	election2, err := New(context.TODO(), "TestElectionOperations", conn2)
	assert.NoError(t, err)
	assert.NotNil(t, election2)

	election3, err := New(context.TODO(), "TestElectionOperations", conn3)
	assert.NoError(t, err)
	assert.NotNil(t, election3)

	ch := make(chan Event)
	err = election1.Watch(context.TODO(), ch)
	assert.NoError(t, err)

	term, err := election1.GetTerm(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(0), term.Revision)
	assert.Equal(t, "", term.Leader)
	assert.Len(t, term.Candidates, 0)

	term, err = election1.Enter(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(1), term.Revision)
	assert.Equal(t, election1.ID(), term.Leader)
	assert.Len(t, term.Candidates, 1)
	assert.Equal(t, election1.ID(), term.Candidates[0])

	event := <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(1), event.Term.Revision)
	assert.Equal(t, election1.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 1)
	assert.Equal(t, election1.ID(), event.Term.Candidates[0])

	term, err = election2.Enter(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(1), term.Revision)
	assert.Equal(t, election1.ID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election1.ID(), term.Candidates[0])
	assert.Equal(t, election2.ID(), term.Candidates[1])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(1), event.Term.Revision)
	assert.Equal(t, election1.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 2)
	assert.Equal(t, election1.ID(), event.Term.Candidates[0])
	assert.Equal(t, election2.ID(), event.Term.Candidates[1])

	term, err = election3.Enter(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(1), term.Revision)
	assert.Equal(t, election1.ID(), term.Leader)
	assert.Len(t, term.Candidates, 3)
	assert.Equal(t, election1.ID(), term.Candidates[0])
	assert.Equal(t, election2.ID(), term.Candidates[1])
	assert.Equal(t, election3.ID(), term.Candidates[2])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(1), event.Term.Revision)
	assert.Equal(t, election1.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 3)
	assert.Equal(t, election1.ID(), event.Term.Candidates[0])
	assert.Equal(t, election2.ID(), event.Term.Candidates[1])
	assert.Equal(t, election3.ID(), event.Term.Candidates[2])

	term, err = election3.Promote(context.TODO(), election3.ID())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(1), term.Revision)
	assert.Equal(t, election1.ID(), term.Leader)
	assert.Len(t, term.Candidates, 3)
	assert.Equal(t, election1.ID(), term.Candidates[0])
	assert.Equal(t, election3.ID(), term.Candidates[1])
	assert.Equal(t, election2.ID(), term.Candidates[2])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(1), event.Term.Revision)
	assert.Equal(t, election1.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 3)
	assert.Equal(t, election1.ID(), event.Term.Candidates[0])
	assert.Equal(t, election3.ID(), event.Term.Candidates[1])
	assert.Equal(t, election2.ID(), event.Term.Candidates[2])

	term, err = election3.Promote(context.TODO(), election3.ID())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(2), term.Revision)
	assert.Equal(t, election3.ID(), term.Leader)
	assert.Len(t, term.Candidates, 3)
	assert.Equal(t, election3.ID(), term.Candidates[0])
	assert.Equal(t, election1.ID(), term.Candidates[1])
	assert.Equal(t, election2.ID(), term.Candidates[2])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(2), event.Term.Revision)
	assert.Equal(t, election3.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 3)
	assert.Equal(t, election3.ID(), event.Term.Candidates[0])
	assert.Equal(t, election1.ID(), event.Term.Candidates[1])
	assert.Equal(t, election2.ID(), event.Term.Candidates[2])

	term, err = election2.Anoint(context.TODO(), election2.ID())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(3), term.Revision)
	assert.Equal(t, election2.ID(), term.Leader)
	assert.Len(t, term.Candidates, 3)
	assert.Equal(t, election2.ID(), term.Candidates[0])
	assert.Equal(t, election3.ID(), term.Candidates[1])
	assert.Equal(t, election1.ID(), term.Candidates[2])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(3), event.Term.Revision)
	assert.Equal(t, election2.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 3)
	assert.Equal(t, election2.ID(), event.Term.Candidates[0])
	assert.Equal(t, election3.ID(), event.Term.Candidates[1])
	assert.Equal(t, election1.ID(), event.Term.Candidates[2])

	term, err = election2.Leave(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(4), term.Revision)
	assert.Equal(t, election3.ID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election3.ID(), term.Candidates[0])
	assert.Equal(t, election1.ID(), term.Candidates[1])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(4), event.Term.Revision)
	assert.Equal(t, election3.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 2)
	assert.Equal(t, election3.ID(), event.Term.Candidates[0])
	assert.Equal(t, election1.ID(), event.Term.Candidates[1])

	term, err = election3.Evict(context.TODO(), election3.ID())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(5), term.Revision)
	assert.Equal(t, election1.ID(), term.Leader)
	assert.Len(t, term.Candidates, 1)
	assert.Equal(t, election1.ID(), term.Candidates[0])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(5), event.Term.Revision)
	assert.Equal(t, election1.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 1)
	assert.Equal(t, election1.ID(), event.Term.Candidates[0])

	term, err = election2.Enter(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(5), term.Revision)
	assert.Equal(t, election1.ID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election1.ID(), term.Candidates[0])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(5), event.Term.Revision)
	assert.Equal(t, election1.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 2)
	assert.Equal(t, election1.ID(), event.Term.Candidates[0])

	term, err = election1.Anoint(context.TODO(), election2.ID())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(6), term.Revision)
	assert.Equal(t, election2.ID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election2.ID(), term.Candidates[0])

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(6), event.Term.Revision)
	assert.Equal(t, election2.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 2)
	assert.Equal(t, election2.ID(), event.Term.Candidates[0])

	err = election2.Close(context.Background())
	assert.NoError(t, err)

	event = <-ch
	assert.Equal(t, EventChange, event.Type)
	assert.Equal(t, meta.Revision(7), event.Term.Revision)
	assert.Equal(t, election1.ID(), event.Term.Leader)
	assert.Len(t, event.Term.Candidates, 1)
	assert.Equal(t, election1.ID(), event.Term.Candidates[0])

	err = election1.Close(context.Background())
	assert.NoError(t, err)
	err = election3.Close(context.Background())
	assert.NoError(t, err)

	election1, err = New(context.TODO(), "TestElectionOperations", conn1)
	assert.NoError(t, err)

	term, err = election1.GetTerm(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, meta.Revision(7), term.Revision)
	assert.Equal(t, "", term.Leader)
	assert.Len(t, term.Candidates, 0)

	err = election1.Close(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, test.Stop())
}
