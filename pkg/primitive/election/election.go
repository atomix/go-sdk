// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	"github.com/atomix/go-client/pkg/primitive"
	"github.com/atomix/go-client/pkg/stream"
	electionv1 "github.com/atomix/runtime/api/atomix/runtime/election/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"io"
)

var log = logging.GetLogger()

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

// electionPrimitive is the single partition implementation of Election
type electionPrimitive struct {
	primitive.Primitive
	client      electionv1.LeaderElectionClient
	candidateID string
}

func (e *electionPrimitive) CandidateID() string {
	return e.candidateID
}

func (e *electionPrimitive) GetTerm(ctx context.Context) (*Term, error) {
	request := &electionv1.GetTermRequest{
		ID: runtimev1.PrimitiveId{
			Name: e.Name(),
		},
	}
	response, err := e.client.GetTerm(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Enter(ctx context.Context) (*Term, error) {
	request := &electionv1.EnterRequest{
		ID: runtimev1.PrimitiveId{
			Name: e.Name(),
		},
		Candidate: e.candidateID,
	}
	response, err := e.client.Enter(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Leave(ctx context.Context) (*Term, error) {
	request := &electionv1.WithdrawRequest{
		ID: runtimev1.PrimitiveId{
			Name: e.Name(),
		},
		Candidate: e.candidateID,
	}
	response, err := e.client.Withdraw(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Anoint(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.AnointRequest{
		ID: runtimev1.PrimitiveId{
			Name: e.Name(),
		},
		Candidate: id,
	}
	response, err := e.client.Anoint(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Promote(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.PromoteRequest{
		ID: runtimev1.PrimitiveId{
			Name: e.Name(),
		},
		Candidate: id,
	}
	response, err := e.client.Promote(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Evict(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.EvictRequest{
		ID: runtimev1.PrimitiveId{
			Name: e.Name(),
		},
		Candidate: id,
	}
	response, err := e.client.Evict(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Watch(ctx context.Context) (TermStream, error) {
	request := &electionv1.WatchRequest{
		ID: runtimev1.PrimitiveId{
			Name: e.Name(),
		},
	}
	client, err := e.client.Watch(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}

	ch := make(chan stream.Result[*Term])
	go func() {
		defer close(ch)
		for {
			response, err := client.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				err = errors.FromProto(err)
				if errors.IsCanceled(err) || errors.IsTimeout(err) {
					return
				}
				log.Errorf("Watch failed: %v", err)
				return
			}
			ch <- stream.Result[*Term]{
				Value: newTerm(&response.Term),
			}
		}
	}()
	return stream.NewChannelStream[*Term](ch), nil
}

func (e *electionPrimitive) create(ctx context.Context, tags map[string]string) error {
	request := &electionv1.CreateRequest{
		ID: runtimev1.PrimitiveId{
			Name: e.Name(),
		},
		Tags: tags,
	}
	_, err := e.client.Create(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func (e *electionPrimitive) Close(ctx context.Context) error {
	request := &electionv1.CloseRequest{
		ID: runtimev1.PrimitiveId{
			Name: e.Name(),
		},
	}
	_, err := e.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
