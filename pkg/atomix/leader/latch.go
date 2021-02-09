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
	api "github.com/atomix/api/go/atomix/primitive/leader"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/meta"
	"google.golang.org/grpc"
	"io"
)

var log = logging.GetLogger("atomix", "client", "leader")

// Type is the latch type
const Type primitive.Type = "LeaderLatch"

// Client provides an API for creating Latches
type Client interface {
	// GetLatch gets the Latch instance of the given name
	GetLatch(ctx context.Context, name string, opts ...Option) (Latch, error)
}

// Latch provides distributed leader latch
type Latch interface {
	primitive.Primitive

	// ID returns the latch identifier
	ID() string

	// Get gets the current latch
	Get(ctx context.Context) (*Leadership, error)

	// Latch attempts to acquire the latch
	Latch(ctx context.Context) (*Leadership, error)

	// Watch watches the latch for changes
	Watch(ctx context.Context, c chan<- Event) error
}

// newLeadership returns a new leadership from the response latch
func newLatch(term *api.Latch) *Leadership {
	if term == nil {
		return nil
	}
	return &Leadership{
		ObjectMeta:   meta.FromProto(term.ObjectMeta),
		Leader:       term.Leader,
		Participants: term.Participants,
	}
}

// Leadership is a leadership term
// A term is guaranteed to have a monotonically increasing, globally unique ID.
type Leadership struct {
	meta.ObjectMeta

	// Leader is the ID of the leader that was elected
	Leader string

	// Participants is a list of candidates currently participating in the latch
	Participants []string
}

// EventType is the type of an Latch event
type EventType string

const (
	// EventChange indicates the latch term changed
	EventChange EventType = "change"
)

// Event is an latch event
type Event struct {
	// Type is the type of the event
	Type EventType

	// Leadership is the term that occurs as a result of the latch event
	Leadership Leadership
}

// New creates a new latch primitive
func New(ctx context.Context, name string, conn *grpc.ClientConn, opts ...Option) (Latch, error) {
	l := &latch{
		Client: primitive.NewClient(Type, name, conn),
		client: api.NewLeaderLatchServiceClient(conn),
	}
	if err := l.Create(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// latch is the default single-partition implementation of Latch
type latch struct {
	*primitive.Client
	id     string
	client api.LeaderLatchServiceClient
}

func (l *latch) ID() string {
	return l.id
}

func (l *latch) Get(ctx context.Context) (*Leadership, error) {
	request := &api.GetRequest{
		Headers: l.GetHeaders(),
	}
	response, err := l.client.Get(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newLatch(&response.Latch), nil
}

func (l *latch) Latch(ctx context.Context) (*Leadership, error) {
	request := &api.LatchRequest{
		Headers: l.GetHeaders(),
	}
	response, err := l.client.Latch(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return newLatch(&response.Latch), nil
}

func (l *latch) Watch(ctx context.Context, ch chan<- Event) error {
	request := &api.EventsRequest{
		Headers: l.GetHeaders(),
	}
	stream, err := l.client.Events(ctx, request)
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
				case api.Event_CHANGE:
					ch <- Event{
						Type:       EventChange,
						Leadership: *newLatch(&response.Event.Latch),
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
