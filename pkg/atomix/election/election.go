// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/time"
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
	Watch(ctx context.Context, ch chan<- Event) error
}

// newTerm returns a new term from the response term
func newTerm(term *electionv1.Term) *Term {
	if term == nil {
		return nil
	}
	return &Term{
		Leader:     term.CandidateID,
		Candidates: term.Candidates,
		Timestamp:  time.NewTimestamp(*term.Timestamp),
	}
}

// Term is a leadership term
// A term is guaranteed to have a monotonically increasing, globally unique ID.
type Term struct {
	// Leader is the ID of the leader that was elected
	Leader string

	// Candidates is a list of candidates currently participating in the election
	Candidates []string

	// Timestamp is the timestamp at which the leader was elected
	Timestamp time.Timestamp
}

// EventType is the type of an Election event
type EventType string

const (
	// EventChange indicates the election term changed
	EventChange EventType = "change"
)

// Event is an election event
type Event struct {
	// Type is the type of the event
	Type EventType

	// Term is the term that occurs as a result of the election event
	Term Term
}

func New(client electionv1.LeaderElectionClient) func(context.Context, primitive.ID, ...Option) (Election, error) {
	return func(ctx context.Context, id primitive.ID, opts ...Option) (Election, error) {
		var options Options
		options.apply(opts...)
		election := &electionPrimitive{
			Primitive:   primitive.New(id),
			client:      client,
			candidateID: options.CandidateID,
		}
		if err := election.create(ctx); err != nil {
			return nil, err
		}
		return election, nil
	}
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
	request := &electionv1.GetTermRequest{}
	ctx = primitive.AppendToOutgoingContext(ctx, e.ID())
	response, err := e.client.GetTerm(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Enter(ctx context.Context) (*Term, error) {
	request := &electionv1.EnterRequest{
		CandidateID: e.candidateID,
	}
	ctx = primitive.AppendToOutgoingContext(ctx, e.ID())
	response, err := e.client.Enter(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Leave(ctx context.Context) (*Term, error) {
	request := &electionv1.WithdrawRequest{
		CandidateID: e.candidateID,
	}
	ctx = primitive.AppendToOutgoingContext(ctx, e.ID())
	response, err := e.client.Withdraw(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Anoint(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.AnointRequest{
		CandidateID: id,
	}
	ctx = primitive.AppendToOutgoingContext(ctx, e.ID())
	response, err := e.client.Anoint(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Promote(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.PromoteRequest{
		CandidateID: id,
	}
	ctx = primitive.AppendToOutgoingContext(ctx, e.ID())
	response, err := e.client.Promote(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Evict(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.EvictRequest{
		CandidateID: id,
	}
	ctx = primitive.AppendToOutgoingContext(ctx, e.ID())
	response, err := e.client.Evict(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *electionPrimitive) Watch(ctx context.Context, ch chan<- Event) error {
	request := &electionv1.EventsRequest{}
	ctx = primitive.AppendToOutgoingContext(ctx, e.ID())
	stream, err := e.client.Events(ctx, request)
	if err != nil {
		return errors.FromProto(err)
	}

	openCh := make(chan struct{})
	go func() {
		defer close(ch)
		open := false
		defer func() {
			if !open {
				close(openCh)
			}
		}()
		for {
			response, err := stream.Recv()
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

			if !open {
				close(openCh)
				open = true
			}

			switch response.Event.Type {
			case electionv1.Event_CHANGED:
				ch <- Event{
					Type: EventChange,
					Term: *newTerm(&response.Event.Term),
				}
			}
		}
	}()

	select {
	case <-openCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *electionPrimitive) create(ctx context.Context) error {
	request := &electionv1.CreateRequest{
		Config: electionv1.LeaderElectionConfig{},
	}
	ctx = primitive.AppendToOutgoingContext(ctx, e.ID())
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
	request := &electionv1.CloseRequest{}
	ctx = primitive.AppendToOutgoingContext(ctx, e.ID())
	_, err := e.client.Close(ctx, request)
	if err != nil {
		err = errors.FromProto(err)
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
