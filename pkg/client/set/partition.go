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

package set

import (
	"context"
	"errors"
	api "github.com/atomix/atomix-api/proto/atomix/set"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

func newPartition(ctx context.Context, conn *grpc.ClientConn, name primitive.Name, opts ...session.Option) (Set, error) {
	client := api.NewSetServiceClient(conn)
	sess, err := session.New(ctx, name, &sessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &setPartition{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

type setPartition struct {
	name    primitive.Name
	client  api.SetServiceClient
	session *session.Session
}

func (s *setPartition) Name() primitive.Name {
	return s.name
}

func (s *setPartition) Add(ctx context.Context, value string) (bool, error) {
	request := &api.AddRequest{
		Header: s.session.NextRequest(),
		Value:  value,
	}

	response, err := s.client.Add(ctx, request)
	if err != nil {
		return false, err
	}

	s.session.RecordResponse(request.Header, response.Header)

	if response.Status == api.ResponseStatus_WRITE_LOCK {
		return false, errors.New("write lock failed")
	}
	return response.Added, nil
}

func (s *setPartition) Remove(ctx context.Context, value string) (bool, error) {
	request := &api.RemoveRequest{
		Header: s.session.NextRequest(),
		Value:  value,
	}

	response, err := s.client.Remove(ctx, request)
	if err != nil {
		return false, err
	}

	s.session.RecordResponse(request.Header, response.Header)

	if response.Status == api.ResponseStatus_WRITE_LOCK {
		return false, errors.New("write lock failed")
	}
	return response.Removed, nil
}

func (s *setPartition) Contains(ctx context.Context, value string) (bool, error) {
	request := &api.ContainsRequest{
		Header: s.session.GetRequest(),
		Value:  value,
	}

	response, err := s.client.Contains(ctx, request)
	if err != nil {
		return false, err
	}

	s.session.RecordResponse(request.Header, response.Header)
	return response.Contains, nil
}

func (s *setPartition) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Header: s.session.GetRequest(),
	}

	response, err := s.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	s.session.RecordResponse(request.Header, response.Header)
	return int(response.Size_), nil
}

func (s *setPartition) Clear(ctx context.Context) error {
	request := &api.ClearRequest{
		Header: s.session.NextRequest(),
	}

	response, err := s.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	s.session.RecordResponse(request.Header, response.Header)
	return nil
}

func (s *setPartition) Watch(ctx context.Context, ch chan<- *Event, opts ...WatchOption) error {
	request := &api.EventRequest{
		Header: s.session.NextRequest(),
	}

	for _, opt := range opts {
		opt.beforeWatch(request)
	}

	events, err := s.client.Events(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		defer close(ch)
		var stream *session.Stream
		for {
			response, err := events.Recv()
			if err == io.EOF {
				if stream != nil {
					stream.Close()
				}
				break
			}

			if err != nil {
				glog.Error("Failed to receive event stream", err)
				break
			}

			for _, opt := range opts {
				opt.afterWatch(response)
			}

			// Record the response header
			s.session.RecordResponse(request.Header, response.Header)

			// Initialize the session stream if necessary.
			if stream == nil {
				stream = s.session.NewStream(response.Header.StreamID)
			}

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			var t EventType
			switch response.Type {
			case api.EventResponse_ADDED:
				t = EventAdded
			case api.EventResponse_REMOVED:
				t = EventRemoved
			}

			ch <- &Event{
				Type:  t,
				Value: response.Value,
			}
		}
	}()
	return nil
}

func (s *setPartition) Close() error {
	return s.session.Close()
}

func (s *setPartition) Delete() error {
	return s.session.Delete()
}
