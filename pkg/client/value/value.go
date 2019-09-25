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

package value

import (
	"context"
	"errors"
	api "github.com/atomix/atomix-api/proto/atomix/value"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

// Type is the value type
const Type primitive.Type = "value"

// Client provides an API for creating Values
type Client interface {
	// GetValue gets the Value instance of the given name
	GetValue(ctx context.Context, name string, opts ...session.Option) (Value, error)
}

// Value provides a simple atomic value
type Value interface {
	primitive.Primitive

	// Set sets the current value and returns the version
	Set(ctx context.Context, value []byte, opts ...SetOption) (uint64, error)

	// Get gets the current value and version
	Get(ctx context.Context) ([]byte, uint64, error)

	// Watch watches the value for changes
	Watch(ctx context.Context, ch chan<- *Event) error
}

// EventType is the type of a set event
type EventType string

const (
	// EventUpdated indicates the value was updated
	EventUpdated EventType = "updated"
)

// Event is a value change event
type Event struct {
	// Type is the change event type
	Type EventType

	// Value is the updated value
	Value []byte

	// Version is the updated version
	Version uint64
}

// New creates a new Lock primitive for the given partitions
// The value will be created in one of the given partitions.
func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.Option) (Value, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}
	return newValue(ctx, name, partitions[i], opts...)
}

// newValue creates a new Value primitive for the given partition
func newValue(ctx context.Context, name primitive.Name, conn *grpc.ClientConn, opts ...session.Option) (*value, error) {
	client := api.NewValueServiceClient(conn)
	sess, err := session.New(ctx, name, &sessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &value{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

// value is the single partition implementation of Lock
type value struct {
	name    primitive.Name
	client  api.ValueServiceClient
	session *session.Session
}

func (v *value) Name() primitive.Name {
	return v.name
}

func (v *value) Set(ctx context.Context, value []byte, opts ...SetOption) (uint64, error) {
	request := &api.SetRequest{
		Header: v.session.NextRequest(),
		Value:  value,
	}

	for _, opt := range opts {
		opt.beforeSet(request)
	}

	response, err := v.client.Set(ctx, request)
	if err != nil {
		return 0, err
	}

	for _, opt := range opts {
		opt.afterSet(response)
	}

	v.session.RecordResponse(request.Header, response.Header)

	if !response.Succeeded {
		if request.ExpectVersion > 0 {
			return 0, errors.New("version mismatch")
		}
		return 0, errors.New("value mismatch")
	}

	return response.Version, nil
}

func (v *value) Get(ctx context.Context) ([]byte, uint64, error) {
	request := &api.GetRequest{
		Header: v.session.GetRequest(),
	}

	response, err := v.client.Get(ctx, request)
	if err != nil {
		return nil, 0, err
	}

	v.session.RecordResponse(request.Header, response.Header)
	return response.Value, response.Version, nil
}

func (v *value) Watch(ctx context.Context, ch chan<- *Event) error {
	request := &api.EventRequest{
		Header: v.session.NextRequest(),
	}

	stream := v.session.NewStream(request.Header.RequestID)

	events, err := v.client.Events(ctx, request)
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
			v.session.RecordResponse(request.Header, response.Header)

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			ch <- &Event{
				Type:    EventUpdated,
				Value:   response.NewValue,
				Version: response.NewVersion,
			}
		}
	}()
	return nil
}

func (v *value) Close() error {
	return v.session.Close()
}

func (v *value) Delete() error {
	return v.session.Delete()
}
