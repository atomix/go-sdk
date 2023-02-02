// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	electionv1 "github.com/atomix/atomix/api/runtime/election/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
)

var log = logging.GetLogger()

type Builder interface {
	primitive.Builder[Builder, Election]
	CandidateID(candidateID string) Builder
}

// Election provides distributed leader election
type Election interface {
	primitive.Primitive

	// CandidateID returns the candidate identifier
	CandidateID() string

	// GetTerm gets the current election term
	GetTerm(ctx context.Context) (*Term, error)

	// Enter enters the instance into the election
	Enter(ctx context.Context) (*Term, error)

	// Leave removes the instance from the election
	Leave(ctx context.Context) (*Term, error)

	// Anoint assigns leadership to the instance with the given ID
	Anoint(ctx context.Context, id string) (*Term, error)

	// Promote increases the priority of the instance with the given ID in the election queue
	Promote(ctx context.Context, id string) (*Term, error)

	// Evict removes the instance with the given ID from the election
	Evict(ctx context.Context, id string) (*Term, error)

	// Watch watches the election for changes
	Watch(ctx context.Context) (TermStream, error)
}

type TermStream stream.Stream[*Term]

// newTerm returns a new term from the response term
func newTerm(term *electionv1.Term) *Term {
	if term == nil {
		return nil
	}
	return &Term{
		ID:         term.Term,
		Leader:     term.Leader,
		Candidates: term.Candidates,
	}
}

// Term is a leadership term
// A term is guaranteed to have a monotonically increasing, globally unique ID.
type Term struct {
	// ID is the monotonically increasing term identifier
	ID uint64

	// Leader is the ID of the leader that was elected
	Leader string

	// Candidates is a list of candidates currently participating in the election
	Candidates []string
}
