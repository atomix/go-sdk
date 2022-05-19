// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/meta"
	"google.golang.org/grpc"
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
		ObjectMeta: meta.FromProto(term.ObjectMeta),
		Leader:     term.Leader,
		Candidates: term.Candidates,
	}
}

// Term is a leadership term
// A term is guaranteed to have a monotonically increasing, globally unique ID.
type Term struct {
	meta.ObjectMeta

	// Leader is the ID of the leader that was elected
	Leader uint64

	// Candidates is a list of candidates currently participating in the election
	Candidates []uint64
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

func Client(conn *grpc.ClientConn) primitive.Client[Election, Option] {
	return primitive.NewClient[Election, Option](newManager(conn), func(primitive *primitive.ManagedPrimitive, opts ...Option) (Election, error) {
		return &election{
			ManagedPrimitive: primitive,
			client:           electionv1.NewLeaderElectionClient(conn),
		}, nil
	})
}

// election is the single partition implementation of Election
type election struct {
	*primitive.ManagedPrimitive
	client      electionv1.LeaderElectionClient
	candidateID string
}

func (e *election) CandidateID() string {
	return e.candidateID
}

func (e *election) GetTerm(ctx context.Context) (*Term, error) {
	request := &electionv1.GetTermRequest{
		Headers: e.GetHeaders(),
	}
	response, err := e.client.GetTerm(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Enter(ctx context.Context) (*Term, error) {
	request := &electionv1.EnterRequest{
		Headers: e.GetHeaders(),
		EnterInput: electionv1.EnterInput{
			CandidateID: e.candidateID,
		},
	}
	response, err := e.client.Enter(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Leave(ctx context.Context) (*Term, error) {
	request := &electionv1.WithdrawRequest{
		Headers: e.GetHeaders(),
		WithdrawInput: electionv1.WithdrawInput{
			CandidateID: e.candidateID,
		},
	}
	response, err := e.client.Withdraw(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Anoint(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.AnointRequest{
		Headers: e.GetHeaders(),
		AnointInput: electionv1.AnointInput{
			CandidateID: id,
		},
	}
	response, err := e.client.Anoint(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Promote(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.PromoteRequest{
		Headers: e.GetHeaders(),
		PromoteInput: electionv1.PromoteInput{
			CandidateID: id,
		},
	}
	response, err := e.client.Promote(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Evict(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.EvictRequest{
		Headers: e.GetHeaders(),
		EvictInput: electionv1.EvictInput{
			CandidateID: id,
		},
	}
	response, err := e.client.Evict(ctx, request)
	if err != nil {
		return nil, errors.FromProto(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Watch(ctx context.Context, ch chan<- Event) error {
	request := &electionv1.EventsRequest{
		Headers: e.GetHeaders(),
	}
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
