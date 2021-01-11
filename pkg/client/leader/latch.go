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
	"github.com/atomix/go-client/pkg/client/meta"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/atomix/go-framework/pkg/atomix/client"
	leaderclient "github.com/atomix/go-framework/pkg/atomix/client/leader"
	"google.golang.org/grpc"
)

// Type is the leader latch type
const Type = primitive.Type(leaderclient.PrimitiveType)

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

// newLeadership returns a new leadership from the output latch
func newLatch(term *api.Latch) *Leadership {
	if term == nil {
		return nil
	}
	return &Leadership{
		ObjectMeta:   meta.New(term.Meta),
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
	options := applyOptions(opts...)
	l := &latch{
		client: leaderclient.NewClient(client.ID(options.clientID), name, conn),
	}
	if err := l.create(ctx); err != nil {
		return nil, err
	}
	return l, nil
}

// latch is the default single-partition implementation of Latch
type latch struct {
	id     string
	client leaderclient.Client
}

func (l *latch) ID() string {
	return string(l.client.ID())
}

func (l *latch) Type() primitive.Type {
	return Type
}

func (l *latch) Name() string {
	return l.client.Name()
}

func (l *latch) Get(ctx context.Context) (*Leadership, error) {
	input := &api.GetInput{
	}
	output, err := l.client.Get(ctx, input)
	if err != nil {
		return nil, err
	}
	return newLatch(output.Latch), nil
}

func (l *latch) Latch(ctx context.Context) (*Leadership, error) {
	input := &api.LatchInput{}
	output, err := l.client.Latch(ctx, input)
	if err != nil {
		return nil, err
	}
	return newLatch(output.Latch), nil
}

func (l *latch) Watch(ctx context.Context, ch chan<- Event) error {
	input := &api.EventsInput{}
	outputCh := make(chan api.EventsOutput)
	if err := l.client.Events(ctx, input, outputCh); err != nil {
		return err
	}
	go func() {
		for output := range outputCh {
			switch output.Type {
			case api.EventsOutput_CHANGE:
				ch <- Event{
					Type:       EventChange,
					Leadership: *newLatch(output.Latch),
				}
			}
		}
	}()
	return nil
}

func (l *latch) create(ctx context.Context) error {
	return l.client.Create(ctx)
}

func (l *latch) Close(ctx context.Context) error {
	return l.client.Close(ctx)
}

func (l *latch) Delete(ctx context.Context) error {
	return l.client.Delete(ctx)
}
