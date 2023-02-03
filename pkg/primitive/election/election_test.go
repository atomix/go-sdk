// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/test"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestElectionOperations(t *testing.T) {
	logging.SetLevel(logging.DebugLevel)

	cluster := test.NewClient()
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	election1, err := NewBuilder(cluster, "test").
		CandidateID("candidate-1").
		Get(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, election1)

	election2, err := NewBuilder(cluster, "test").
		CandidateID("candidate-2").
		Get(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, election2)

	election3, err := NewBuilder(cluster, "test").
		CandidateID("candidate-3").
		Get(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, election3)

	terms, err := election1.Watch(context.TODO())
	assert.NoError(t, err)

	term, err := election1.GetTerm(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), term.ID)
	assert.Equal(t, "", term.Leader)
	assert.Len(t, term.Candidates, 0)

	term, err = election1.Enter(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 0)

	term, err = terms.Next()
	assert.Equal(t, uint64(1), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 0)

	term, err = election2.Enter(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 1)
	assert.Equal(t, election2.CandidateID(), term.Candidates[0])

	term, err = terms.Next()
	assert.Equal(t, uint64(1), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 1)
	assert.Equal(t, election2.CandidateID(), term.Candidates[0])

	term, err = election3.Enter(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election2.CandidateID(), term.Candidates[0])
	assert.Equal(t, election3.CandidateID(), term.Candidates[1])

	term, err = terms.Next()
	assert.Equal(t, uint64(1), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election2.CandidateID(), term.Candidates[0])
	assert.Equal(t, election3.CandidateID(), term.Candidates[1])

	term, err = election3.Promote(context.TODO(), election3.CandidateID())
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election3.CandidateID(), term.Candidates[0])
	assert.Equal(t, election2.CandidateID(), term.Candidates[1])

	term, err = terms.Next()
	assert.Equal(t, uint64(1), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election3.CandidateID(), term.Candidates[0])
	assert.Equal(t, election2.CandidateID(), term.Candidates[1])

	term, err = election3.Promote(context.TODO(), election3.CandidateID())
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), term.ID)
	assert.Equal(t, election3.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election1.CandidateID(), term.Candidates[0])
	assert.Equal(t, election2.CandidateID(), term.Candidates[1])

	term, err = terms.Next()
	assert.Equal(t, uint64(2), term.ID)
	assert.Equal(t, election3.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election1.CandidateID(), term.Candidates[0])
	assert.Equal(t, election2.CandidateID(), term.Candidates[1])

	term, err = election2.Anoint(context.TODO(), election2.CandidateID())
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), term.ID)
	assert.Equal(t, election2.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election3.CandidateID(), term.Candidates[0])
	assert.Equal(t, election1.CandidateID(), term.Candidates[1])

	term, err = terms.Next()
	assert.Equal(t, uint64(3), term.ID)
	assert.Equal(t, election2.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 2)
	assert.Equal(t, election3.CandidateID(), term.Candidates[0])
	assert.Equal(t, election1.CandidateID(), term.Candidates[1])

	term, err = election2.Leave(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), term.ID)
	assert.Equal(t, election3.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 1)
	assert.Equal(t, election1.CandidateID(), term.Candidates[0])

	term, err = terms.Next()
	assert.Equal(t, uint64(4), term.ID)
	assert.Equal(t, election3.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 1)
	assert.Equal(t, election1.CandidateID(), term.Candidates[0])

	term, err = election3.Evict(context.TODO(), election3.CandidateID())
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 0)

	term, err = terms.Next()
	assert.Equal(t, uint64(5), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 0)

	term, err = election2.Enter(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 1)

	term, err = terms.Next()
	assert.Equal(t, uint64(5), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 1)

	term, err = election1.Anoint(context.TODO(), election2.CandidateID())
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), term.ID)
	assert.Equal(t, election2.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 1)

	term, err = terms.Next()
	assert.Equal(t, uint64(6), term.ID)
	assert.Equal(t, election2.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 1)

	err = election2.Close(context.Background())
	assert.NoError(t, err)

	term, err = terms.Next()
	assert.Equal(t, uint64(7), term.ID)
	assert.Equal(t, election1.CandidateID(), term.Leader)
	assert.Len(t, term.Candidates, 0)

	err = election1.Close(context.Background())
	assert.NoError(t, err)
	err = election3.Close(context.Background())
	assert.NoError(t, err)

	election1, err = NewBuilder(cluster, "test").
		CandidateID("candidate-1").
		Get(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, election1)

	election2, err = NewBuilder(cluster, "test").
		CandidateID("candidate-2").
		Get(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, election2)

	term, err = election1.GetTerm(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), term.ID)
	assert.Equal(t, "", term.Leader)
	assert.Len(t, term.Candidates, 0)

	err = election1.Close(context.Background())
	assert.NoError(t, err)
}
