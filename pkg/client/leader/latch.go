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

package leader

import (
	"context"
	"errors"
	api "github.com/atomix/atomix-api/proto/atomix/leader"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
	"time"
)

// Type is the leader latch type
const Type primitive.Type = "leaderlatch"

// Client provides an API for creating Latches
type Client interface {
	// GetLatch gets the Latch instance of the given name
	GetLatch(ctx context.Context, name string, opts ...session.Option) (Latch, error)
}

// Latch provides distributed leader latch
type Latch interface {
	primitive.Primitive

	// ID returns the ID of the instance of the latch
	ID() string

	// Get gets the current latch
	Get(ctx context.Context) (*Leadership, error)

	// Join joins the latch
	Join(ctx context.Context) (*Leadership, error)

	// Latch attempts to acquire the latch
	Latch(ctx context.Context) (*Leadership, error)

	// Watch watches the latch for changes
	Watch(ctx context.Context, c chan<- *Event) error
}

// newLeadership returns a new leadership from the response latch
func newLeadership(term *api.Latch) *Leadership {
	if term == nil {
		return nil
	}
	return &Leadership{
		ID:           term.ID,
		Leader:       term.Leader,
		Participants: term.Participants,
	}
}

// Leadership is a leadership term
// A term is guaranteed to have a monotonically increasing, globally unique ID.
type Leadership struct {
	// ID is a globally unique, monotonically increasing term number
	ID uint64

	// Leader is the ID of the leader that was elected
	Leader string

	// Participants is a list of candidates currently participating in the latch
	Participants []string
}

// EventType is the type of an Latch event
type EventType string

const (
	// EventChanged indicates the latch term changed
	EventChanged EventType = "changed"
)

// Event is an latch event
type Event struct {
	// Type is the type of the event
	Type EventType

	// Leadership is the term that occurs as a result of the latch event
	Leadership Leadership
}

// New creates a new latch primitive
func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.Option) (Latch, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderLatchServiceClient(partitions[i])
	sess, err := session.New(ctx, name, &sessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}

	return &latch{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

// latch is the default single-partition implementation of Latch
type latch struct {
	name    primitive.Name
	client  api.LeaderLatchServiceClient
	session *session.Session
}

func (e *latch) Name() primitive.Name {
	return e.name
}

func (e *latch) ID() string {
	return e.session.ID
}

func (e *latch) Get(ctx context.Context) (*Leadership, error) {
	request := &api.GetRequest{
		Header: e.session.GetRequest(),
	}

	response, err := e.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return newLeadership(response.Latch), nil
}

func (e *latch) Join(ctx context.Context) (*Leadership, error) {
	stream, header := e.session.NextStream()
	defer stream.Close()

	request := &api.LatchRequest{
		Header:        header,
		ParticipantID: e.ID(),
	}

	response, err := e.client.Latch(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return newLeadership(response.Latch), nil
}

func (e *latch) Latch(ctx context.Context) (*Leadership, error) {
	leadership, err := e.Join(ctx)
	if err != nil {
		return nil, err
	} else if leadership.Leader == e.ID() {
		return leadership, nil
	}

	ch := make(chan *Event)
	if err := e.Watch(ctx, ch); err != nil {
		return nil, err
	}

	for event := range ch {
		if event.Leadership.Leader == e.ID() {
			return &event.Leadership, nil
		}
	}
	return nil, errors.New("failed to acquire latch")
}

func (e *latch) Watch(ctx context.Context, ch chan<- *Event) error {
	stream, header := e.session.NextStream()

	request := &api.EventRequest{
		Header: header,
	}

	events, err := e.client.Events(context.Background(), request)
	if err != nil {
		return err
	}

	openCh := make(chan error)
	go func() {
		defer close(ch)
		open := false
		for {
			response, err := events.Recv()
			if err == io.EOF {
				if !open {
					close(openCh)
				}
				stream.Close()
				break
			}

			if err != nil {
				glog.Error("Failed to receive event stream", err)
				if !open {
					openCh <- err
					close(openCh)
				}
				stream.Close()
				break
			}

			// Record the response header
			e.session.RecordResponse(request.Header, response.Header)

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			// Return the Watch call if possible
			if !open {
				close(openCh)
				open = true
			}

			// If this is a normal event (not a handshake response), write the event to the watch channel
			if response.Type != api.EventResponse_OPEN {
				ch <- &Event{
					Type:       EventChanged,
					Leadership: *newLeadership(response.Latch),
				}
			}
		}
	}()

	// Close the stream once the context is cancelled
	closeCh := ctx.Done()
	go func() {
		<-closeCh
		_ = events.CloseSend()
	}()

	// Block the Watch until the handshake is complete or times out
	select {
	case err := <-openCh:
		return err
	case <-time.After(15 * time.Second):
		_ = events.CloseSend()
		return errors.New("handshake timed out")
	}
}

func (e *latch) Close() error {
	return e.session.Close()
}

func (e *latch) Delete() error {
	return e.session.Delete()
}
