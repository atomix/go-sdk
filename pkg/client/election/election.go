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
	api "github.com/atomix/api/go/atomix/primitive/election"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger("atomix", "client", "election")

// Type is the election type
const Type primitive.Type = "Election"

// Client provides an API for creating Elections
type Client interface {
	// GetElection gets the Election instance of the given name
	GetElection(ctx context.Context, name string, opts ...Option) (Election, error)
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
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (Election, error) {
	options := applyOptions(opts...)
	e := &election{
		Client: primitive.NewClient(Type, name, conn),
		id:     options.clientID,
		client: api.NewLeaderElectionServiceClient(conn),
	}
	if err := e.Create(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

// election is the single partition implementation of Election
type election struct {
	*primitive.Client
	id     string
	client api.LeaderElectionServiceClient
}

func (e *election) ID() string {
	return e.id
}

func (e *election) GetTerm(ctx context.Context) (*Term, error) {
	request := &api.GetTermRequest{}
	response, err := e.client.GetTerm(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Enter(ctx context.Context) (*Term, error) {
	request := &api.EnterRequest{}
	response, err := e.client.Enter(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Leave(ctx context.Context) (*Term, error) {
	request := &api.WithdrawRequest{}
	response, err := e.client.Withdraw(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Anoint(ctx context.Context, id string) (*Term, error) {
	request := &api.AnointRequest{
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
		CandidateID: id,
	}
	response, err := e.client.Evict(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newTerm(&response.Term), nil
}

func (e *election) Watch(ctx context.Context, ch chan<- Event) error {
	request := &api.EventsRequest{}
	stream, err := e.client.Events(e.AddHeaders(ctx), request)
	if err != nil {
		return errors.From(err)
	}

	openCh := make(chan struct{})
	go func() {
		defer close(ch)
		open := false
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				log.Errorf("Watch failed: %v", err)
			} else {
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
		}
	}()

	select {
	case <-openCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
