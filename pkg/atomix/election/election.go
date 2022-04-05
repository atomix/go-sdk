// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	api "github.com/atomix/atomix-api/go/atomix/primitive/election"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger("atomix", "client", "election")

// Type is the election type
const Type primitive.Type = "Election"

// Client provides an API for creating Elections
type Client interface {
	// GetElection gets the Election instance of the given name
	GetElection(ctx context.Context, name string, opts ...primitive.Option) (Election, error)
}

// Election provides distributed leader election
type Election interface {
	primitive.Primitive

	// ID returns the election identifier
	ID() string

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
func newTerm(term *api.Term) *Term {
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
	Leader string

	// Candidates is a list of candidates currently participating in the election
	Candidates []string
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

// New creates a new election primitive
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...primitive.Option) (Election, error) {
	options := newElectionOptions{}
	for _, opt := range opts {
		if op, ok := opt.(Option); ok {
			op.applyNewElection(&options)
		}
	}
	e := &election{
		Client:  primitive.NewClient(Type, name, conn, opts...),
		client:  api.NewLeaderElectionServiceClient(conn),
		options: options,
	}
	if err := e.Create(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

// election is the single partition implementation of Election
type election struct {
	*primitive.Client
	client  api.LeaderElectionServiceClient
	options newElectionOptions
}

func (e *election) ID() string {
	return e.SessionID()
}

func (e *election) GetTerm(ctx context.Context) (*Term, error) {
	request := &api.GetTermRequest{
		Headers: e.GetHeaders(),
	}
	response, err := e.client.GetTerm(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Enter(ctx context.Context) (*Term, error) {
	request := &api.EnterRequest{
		Headers:     e.GetHeaders(),
		CandidateID: e.SessionID(),
	}
	response, err := e.client.Enter(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Leave(ctx context.Context) (*Term, error) {
	request := &api.WithdrawRequest{
		Headers:     e.GetHeaders(),
		CandidateID: e.SessionID(),
	}
	response, err := e.client.Withdraw(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Anoint(ctx context.Context, id string) (*Term, error) {
	request := &api.AnointRequest{
		Headers:     e.GetHeaders(),
		CandidateID: id,
	}
	response, err := e.client.Anoint(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Promote(ctx context.Context, id string) (*Term, error) {
	request := &api.PromoteRequest{
		Headers:     e.GetHeaders(),
		CandidateID: id,
	}
	response, err := e.client.Promote(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Evict(ctx context.Context, id string) (*Term, error) {
	request := &api.EvictRequest{
		Headers:     e.GetHeaders(),
		CandidateID: id,
	}
	response, err := e.client.Evict(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Watch(ctx context.Context, ch chan<- Event) error {
	request := &api.EventsRequest{
		Headers: e.GetHeaders(),
	}
	stream, err := e.client.Events(ctx, request)
	if err != nil {
		return errors.From(err)
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
				err = errors.From(err)
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
			case api.Event_CHANGED:
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
