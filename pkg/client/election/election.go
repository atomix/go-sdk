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
	"github.com/atomix/go-client/pkg/client/meta"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-framework/pkg/atomix/client"
	electionclient "github.com/atomix/go-framework/pkg/atomix/client/election"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"google.golang.org/grpc"
)

// Type is the election primitive type
const Type = primitive.Type(electionclient.PrimitiveType)

var log = logging.GetLogger("atomix", "client", "election")

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

// newTerm returns a new term from the output term
func newTerm(term *api.Term) *Term {
	if term == nil {
		return nil
	}
	return &Term{
		ObjectMeta: meta.New(term.Meta),
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
		client: electionclient.NewClient(client.ID(options.clientID), name, conn),
	}
	if err := e.create(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

// election is the single partition implementation of Election
type election struct {
	client electionclient.Client
}

func (e *election) Type() primitive.Type {
	return Type
}

func (e *election) ID() string {
	return string(e.client.ID())
}

func (e *election) Name() string {
	return e.client.Name()
}

func (e *election) GetTerm(ctx context.Context) (*Term, error) {
	input := &api.GetTermInput{}
	output, err := e.client.GetTerm(ctx, input)
	if err != nil {
		return nil, err
	}
	return newTerm(output.Term), nil
}

func (e *election) Enter(ctx context.Context) (*Term, error) {
	input := &api.EnterInput{}
	output, err := e.client.Enter(ctx, input)
	if err != nil {
		return nil, err
	}
	return newTerm(output.Term), nil
}

func (e *election) Leave(ctx context.Context) (*Term, error) {
	input := &api.WithdrawInput{}
	output, err := e.client.Withdraw(ctx, input)
	if err != nil {
		return nil, err
	}
	return newTerm(output.Term), nil
}

func (e *election) Anoint(ctx context.Context, id string) (*Term, error) {
	input := &api.AnointInput{
		CandidateID: id,
	}
	output, err := e.client.Anoint(ctx, input)
	if err != nil {
		return nil, err
	}
	return newTerm(output.Term), nil
}

func (e *election) Promote(ctx context.Context, id string) (*Term, error) {
	input := &api.PromoteInput{
		CandidateID: id,
	}
	output, err := e.client.Promote(ctx, input)
	if err != nil {
		return nil, err
	}
	return newTerm(output.Term), nil
}

func (e *election) Evict(ctx context.Context, id string) (*Term, error) {
	input := &api.EvictInput{
		CandidateID: id,
	}
	output, err := e.client.Evict(ctx, input)
	if err != nil {
		return nil, err
	}
	return newTerm(output.Term), nil
}

func (e *election) Watch(ctx context.Context, ch chan<- Event) error {
	outputCh := make(chan api.EventsOutput)
	input := &api.EventsInput{}
	if err := e.client.Events(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
		defer close(ch)
		for output := range outputCh {
			switch output.Type {
			case api.EventsOutput_CHANGED:
				ch <- Event{
					Type: EventChange,
					Term: *newTerm(output.Term),
				}
			}
		}
	}()
	return nil
}

func (e *election) create(ctx context.Context) error {
	return e.client.Create(ctx)
}

func (e *election) Close(ctx context.Context) error {
	return e.client.Close(ctx)
}

func (e *election) Delete(ctx context.Context) error {
	return e.client.Delete(ctx)
}
