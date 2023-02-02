// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	electionv1 "github.com/atomix/atomix/api/runtime/election/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/go-sdk/pkg/primitive"
	"github.com/atomix/go-sdk/pkg/stream"
	"io"
)

func newElectionsClient(name string, client electionv1.LeaderElectionsClient) primitive.Primitive {
	return &electionsClient{
		name:   name,
		client: client,
	}
}

type electionsClient struct {
	name   string
	client electionv1.LeaderElectionsClient
}

func (s *electionsClient) Name() string {
	return s.name
}

func (s *electionsClient) Close(ctx context.Context) error {
	_, err := s.client.Close(ctx, &electionv1.CloseRequest{})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func newElectionClient(name string, candidateID string, client electionv1.LeaderElectionClient) Election {
	return &electionClient{
		Primitive:   newElectionsClient(name, client),
		client:      client,
		candidateID: candidateID,
	}
}

// electionClient is the single partition implementation of Election
type electionClient struct {
	primitive.Primitive
	client      electionv1.LeaderElectionClient
	candidateID string
}

func (e *electionClient) CandidateID() string {
	return e.candidateID
}

func (e *electionClient) GetTerm(ctx context.Context) (*Term, error) {
	request := &electionv1.GetTermRequest{
		ID: runtimev1.PrimitiveID{
			Name: e.Name(),
		},
	}
	response, err := e.client.GetTerm(ctx, request)
	if err != nil {
		return nil, err
	}
	return newTerm(&response.Term), nil
}

func (e *electionClient) Enter(ctx context.Context) (*Term, error) {
	request := &electionv1.EnterRequest{
		ID: runtimev1.PrimitiveID{
			Name: e.Name(),
		},
		Candidate: e.candidateID,
	}
	response, err := e.client.Enter(ctx, request)
	if err != nil {
		return nil, err
	}
	return newTerm(&response.Term), nil
}

func (e *electionClient) Leave(ctx context.Context) (*Term, error) {
	request := &electionv1.WithdrawRequest{
		ID: runtimev1.PrimitiveID{
			Name: e.Name(),
		},
		Candidate: e.candidateID,
	}
	response, err := e.client.Withdraw(ctx, request)
	if err != nil {
		return nil, err
	}
	return newTerm(&response.Term), nil
}

func (e *electionClient) Anoint(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.AnointRequest{
		ID: runtimev1.PrimitiveID{
			Name: e.Name(),
		},
		Candidate: id,
	}
	response, err := e.client.Anoint(ctx, request)
	if err != nil {
		return nil, err
	}
	return newTerm(&response.Term), nil
}

func (e *electionClient) Promote(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.PromoteRequest{
		ID: runtimev1.PrimitiveID{
			Name: e.Name(),
		},
		Candidate: id,
	}
	response, err := e.client.Promote(ctx, request)
	if err != nil {
		return nil, err
	}
	return newTerm(&response.Term), nil
}

func (e *electionClient) Evict(ctx context.Context, id string) (*Term, error) {
	request := &electionv1.EvictRequest{
		ID: runtimev1.PrimitiveID{
			Name: e.Name(),
		},
		Candidate: id,
	}
	response, err := e.client.Evict(ctx, request)
	if err != nil {
		return nil, err
	}
	return newTerm(&response.Term), nil
}

func (e *electionClient) Watch(ctx context.Context) (TermStream, error) {
	request := &electionv1.WatchRequest{
		ID: runtimev1.PrimitiveID{
			Name: e.Name(),
		},
	}
	client, err := e.client.Watch(ctx, request)
	if err != nil {
		return nil, err
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
