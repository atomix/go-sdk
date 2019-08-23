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

package _map

import (
	"context"
	"errors"
	api "github.com/atomix/atomix-api/proto/atomix/map"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

func newPartition(ctx context.Context, conn *grpc.ClientConn, name primitive.Name, opts ...session.SessionOption) (Map, error) {
	client := api.NewMapServiceClient(conn)
	sess, err := session.New(ctx, name, &SessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &mapPartition{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

type mapPartition struct {
	name    primitive.Name
	client  api.MapServiceClient
	session *session.Session
}

func (m *mapPartition) Name() primitive.Name {
	return m.name
}

func (m *mapPartition) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*KeyValue, error) {
	request := &api.PutRequest{
		Header: m.session.NextRequest(),
		Key:    key,
		Value:  value,
	}

	for i := range opts {
		opts[i].beforePut(request)
	}

	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterPut(response)
	}

	m.session.RecordResponse(request.Header, response.Header)

	if response.Status == api.ResponseStatus_OK {
		return &KeyValue{
			Key:     key,
			Value:   value,
			Version: int64(response.Header.Index),
		}, nil
	} else if response.Status == api.ResponseStatus_PRECONDITION_FAILED {
		return nil, errors.New("write condition failed")
	} else if response.Status == api.ResponseStatus_WRITE_LOCK {
		return nil, errors.New("write lock failed")
	} else {
		return &KeyValue{
			Key:     key,
			Value:   value,
			Version: int64(response.PreviousVersion),
		}, nil
	}
}

func (m *mapPartition) Get(ctx context.Context, key string, opts ...GetOption) (*KeyValue, error) {
	request := &api.GetRequest{
		Header: m.session.GetRequest(),
		Key:    key,
	}

	for i := range opts {
		opts[i].beforeGet(request)
	}

	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterGet(response)
	}

	m.session.RecordResponse(request.Header, response.Header)

	if response.Version != 0 {
		return &KeyValue{
			Key:     key,
			Value:   response.Value,
			Version: response.Version,
		}, nil
	}
	return nil, nil
}

func (m *mapPartition) Remove(ctx context.Context, key string, opts ...RemoveOption) (*KeyValue, error) {
	request := &api.RemoveRequest{
		Header: m.session.NextRequest(),
		Key:    key,
	}

	for i := range opts {
		opts[i].beforeRemove(request)
	}

	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterRemove(response)
	}

	m.session.RecordResponse(request.Header, response.Header)

	if response.Status == api.ResponseStatus_OK {
		return &KeyValue{
			Key:     key,
			Value:   response.PreviousValue,
			Version: response.PreviousVersion,
		}, nil
	} else if response.Status == api.ResponseStatus_PRECONDITION_FAILED {
		return nil, errors.New("write condition failed")
	} else if response.Status == api.ResponseStatus_WRITE_LOCK {
		return nil, errors.New("write lock failed")
	} else {
		return nil, nil
	}
}

func (m *mapPartition) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Header: m.session.GetRequest(),
	}

	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	m.session.RecordResponse(request.Header, response.Header)
	return int(response.Size_), nil
}

func (m *mapPartition) Clear(ctx context.Context) error {
	request := &api.ClearRequest{
		Header: m.session.NextRequest(),
	}

	response, err := m.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	m.session.RecordResponse(request.Header, response.Header)
	return nil
}

func (m *mapPartition) Entries(ctx context.Context, ch chan<- *KeyValue) error {
	request := &api.EntriesRequest{
		Header: m.session.GetRequest(),
	}
	entries, err := m.client.Entries(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		defer close(ch)
		for {
			response, err := entries.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				glog.Error("Failed to receive entry stream", err)
				break
			}

			// Record the response header
			m.session.RecordResponse(request.Header, response.Header)

			ch <- &KeyValue{
				Key:     response.Key,
				Value:   response.Value,
				Version: response.Version,
			}
		}
	}()
	return nil
}

func (m *mapPartition) Watch(ctx context.Context, ch chan<- *MapEvent, opts ...WatchOption) error {
	request := &api.EventRequest{
		Header: m.session.NextRequest(),
	}

	for _, opt := range opts {
		opt.beforeWatch(request)
	}

	events, err := m.client.Events(ctx, request)
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
			m.session.RecordResponse(request.Header, response.Header)

			// Initialize the session stream if necessary.
			if stream == nil {
				stream = m.session.NewStream(response.Header.StreamID)
			}

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			switch response.Type {
			case api.EventResponse_NONE:
				ch <- &MapEvent{
					Type:    EventNone,
					Key:     response.Key,
					Value:   response.NewValue,
					Version: response.NewVersion,
				}
			case api.EventResponse_INSERTED:
				ch <- &MapEvent{
					Type:    EventInserted,
					Key:     response.Key,
					Value:   response.NewValue,
					Version: response.NewVersion,
				}
			case api.EventResponse_UPDATED:
				ch <- &MapEvent{
					Type:    EventUpdated,
					Key:     response.Key,
					Value:   response.NewValue,
					Version: response.NewVersion,
				}
			case api.EventResponse_REMOVED:
				ch <- &MapEvent{
					Type:    EventRemoved,
					Key:     response.Key,
					Value:   response.OldValue,
					Version: response.OldVersion,
				}
			}
		}
	}()
	return nil
}

func (m *mapPartition) Close() error {
	return m.session.Close()
}

func (m *mapPartition) Delete() error {
	return m.session.Delete()
}
