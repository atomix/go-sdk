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
	api "github.com/atomix/atomix-api/proto/atomix/election"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"io"
)

// Type is the election type
const Type primitive.Type = "election"

// Client provides an API for creating Elections
type Client interface {
	// GetElection gets the Election instance of the given name
	GetElection(ctx context.Context, name string, opts ...session.Option) (Election, error)
}

// Election provides distributed leader election
type Election interface {
	primitive.Primitive
	ID() string
	GetTerm(ctx context.Context) (*Term, error)
	Enter(ctx context.Context) (*Term, error)
	Leave(ctx context.Context) (*Term, error)
	Anoint(ctx context.Context, id string) (*Term, error)
	Promote(ctx context.Context, id string) (*Term, error)
	Evict(ctx context.Context, id string) (*Term, error)
	Watch(ctx context.Context, c chan<- *Event) error
}

// newTerm returns a new term from the response term
func newTerm(term *api.Term) *Term {
	if term == nil {
		return nil
	}
	return &Term{
		ID:         term.ID,
		Leader:     term.Leader,
		Candidates: term.Candidates,
	}
}

// Term is a leadership term
// A term is guaranteed to have a monotonically increasing, globally unique ID.
type Term struct {
	// ID is a globally unique, monotonically increasing term number
	ID uint64

	// Leader is the ID of the leader that was elected
	Leader string

	// Candidates is a list of candidates currently participating in the election
	Candidates []string
}

// EventType is the type of an Election event
type EventType string

const (
	// EventChanged indicates the election term changed
	EventChanged EventType = "changed"
)

// Event is an election event
type Event struct {
	// Type is the type of the event
	Type EventType

	// Term is the term that occurs as a result of the election event
	Term Term
}

// New creates a new election primitive
func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.Option) (Election, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderElectionServiceClient(partitions[i])
	sess, err := session.New(ctx, name, &sessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}

	return &election{
		name:    name,
		client:  client,
		session: sess,
		id:      uuid.New().String(),
	}, nil
}

// election is the default single-partition implementation of Election
type election struct {
	name    primitive.Name
	client  api.LeaderElectionServiceClient
	session *session.Session
	id      string
}

func (e *election) Name() primitive.Name {
	return e.name
}

func (e *election) ID() string {
	return e.id
}

func (e *election) GetTerm(ctx context.Context) (*Term, error) {
	request := &api.GetTermRequest{
		Header: e.session.GetRequest(),
	}

	response, err := e.client.GetTerm(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return newTerm(response.Term), nil
}

func (e *election) Enter(ctx context.Context) (*Term, error) {
	stream, header := e.session.NextStream()
	defer stream.Close()

	request := &api.EnterRequest{
		Header:      header,
		CandidateID: e.id,
	}

	response, err := e.client.Enter(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return newTerm(response.Term), nil
}

func (e *election) Leave(ctx context.Context) (*Term, error) {
	stream, header := e.session.NextStream()
	defer stream.Close()

	request := &api.WithdrawRequest{
		Header:      header,
		CandidateID: e.id,
	}

	response, err := e.client.Withdraw(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return newTerm(response.Term), nil
}

func (e *election) Anoint(ctx context.Context, id string) (*Term, error) {
	stream, header := e.session.NextStream()
	defer stream.Close()

	request := &api.AnointRequest{
		Header:      header,
		CandidateID: id,
	}

	response, err := e.client.Anoint(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return newTerm(response.Term), nil
}

func (e *election) Promote(ctx context.Context, id string) (*Term, error) {
	stream, header := e.session.NextStream()
	defer stream.Close()

	request := &api.PromoteRequest{
		Header:      header,
		CandidateID: id,
	}

	response, err := e.client.Promote(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return newTerm(response.Term), nil
}

func (e *election) Evict(ctx context.Context, id string) (*Term, error) {
	stream, header := e.session.NextStream()
	defer stream.Close()

	request := &api.EvictRequest{
		Header:      header,
		CandidateID: id,
	}

	response, err := e.client.Evict(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return newTerm(response.Term), nil
}

func (e *election) Watch(ctx context.Context, ch chan<- *Event) error {
	stream, header := e.session.NextStream()

	request := &api.EventRequest{
		Header: header,
	}

	events, err := e.client.Events(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		defer close(ch)
		for {
			response, err := events.Recv()
			if err == io.EOF {
				stream.Close()
				break
			}

			if err != nil {
				glog.Error("Failed to receive event stream", err)
				stream.Close()
				break
			}

			// Record the response header
			e.session.RecordResponse(request.Header, response.Header)

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			ch <- &Event{
				Type: EventChanged,
				Term: *newTerm(response.Term),
			}
		}
	}()
	return nil
}

func (e *election) Close() error {
	return e.session.Close()
}

func (e *election) Delete() error {
	return e.session.Delete()
}
