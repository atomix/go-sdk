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

package _map //nolint:golint

import (
	"context"
	"errors"
	api "github.com/atomix/atomix-api/proto/atomix/map"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/test"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"io"
	"testing"
)

// NewTestServer creates a new server for managing sessions
func NewTestServer() *TestServer {
	return &TestServer{
		Server:  test.NewServer(),
		entries: make(map[string]*KeyValue),
	}
}

type TestServer struct {
	*test.Server
	entries map[string]*KeyValue
}

func (s *TestServer) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	headers, err := s.CreateHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &api.CreateResponse{
		Header: headers,
	}, nil
}

func (s *TestServer) KeepAlive(ctx context.Context, request *api.KeepAliveRequest) (*api.KeepAliveResponse, error) {
	headers, err := s.KeepAliveHeader(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	return &api.KeepAliveResponse{
		Header: headers,
	}, nil
}

func (s *TestServer) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	err := s.CloseHeader(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	return &api.CloseResponse{}, nil
}

func (s *TestServer) Put(ctx context.Context, request *api.PutRequest) (*api.PutResponse, error) {
	index := s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.RequestID
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	v := s.entries[request.Key]

	if request.Version != 0 && (v == nil || v.Version != request.Version) {
		return &api.PutResponse{
			Header: header,
			Status: api.ResponseStatus_PRECONDITION_FAILED,
		}, nil
	}

	if v != nil && valuesEqual(v.Value, request.Value) {
		return &api.PutResponse{
			Header: header,
			Status: api.ResponseStatus_NOOP,
		}, nil
	}

	s.entries[request.Key] = &KeyValue{
		Key:     request.Key,
		Value:   request.Value,
		Version: int64(index),
	}

	streams := session.Streams()
	for _, stream := range streams {
		if v == nil {
			stream.Send(&api.EventResponse{
				Header:     stream.NewResponseHeader(),
				Type:       api.EventResponse_INSERTED,
				Key:        request.Key,
				NewValue:   request.Value,
				NewVersion: request.Version,
			})
		} else {
			stream.Send(&api.EventResponse{
				Header:     stream.NewResponseHeader(),
				Type:       api.EventResponse_UPDATED,
				Key:        request.Key,
				OldValue:   v.Value,
				OldVersion: v.Version,
				NewValue:   request.Value,
				NewVersion: request.Version,
			})
		}
	}

	if v != nil {
		return &api.PutResponse{
			Header:          header,
			Status:          api.ResponseStatus_OK,
			PreviousValue:   v.Value,
			PreviousVersion: v.Version,
		}, nil
	}
	return &api.PutResponse{
		Header: header,
		Status: api.ResponseStatus_OK,
	}, nil
}

func (s *TestServer) Get(ctx context.Context, request *api.GetRequest) (*api.GetResponse, error) {
	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return nil, err
	}

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	v, ok := s.entries[request.Key]
	if ok {
		return &api.GetResponse{
			Header:  header,
			Value:   v.Value,
			Version: v.Version,
		}, nil
	}
	return &api.GetResponse{
		Header: header,
	}, nil
}

func (s *TestServer) Remove(ctx context.Context, request *api.RemoveRequest) (*api.RemoveResponse, error) {
	s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.RequestID
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	v := s.entries[request.Key]

	if v == nil {
		return &api.RemoveResponse{
			Header: header,
			Status: api.ResponseStatus_NOOP,
		}, nil
	}

	if request.Version != 0 && v.Version != request.Version {
		return &api.RemoveResponse{
			Header: header,
			Status: api.ResponseStatus_PRECONDITION_FAILED,
		}, nil
	}

	delete(s.entries, request.Key)

	for _, stream := range session.Streams() {
		stream.Send(&api.EventResponse{
			Header:     stream.NewResponseHeader(),
			Type:       api.EventResponse_REMOVED,
			Key:        request.Key,
			OldValue:   v.Value,
			OldVersion: v.Version,
		})
	}

	if v.Version != 0 {
		return &api.RemoveResponse{
			Header:          header,
			Status:          api.ResponseStatus_OK,
			PreviousValue:   v.Value,
			PreviousVersion: v.Version,
		}, nil
	}
	return &api.RemoveResponse{
		Header: header,
		Status: api.ResponseStatus_OK,
	}, nil
}

func (s *TestServer) Replace(ctx context.Context, request *api.ReplaceRequest) (*api.ReplaceResponse, error) {
	index := s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.RequestID
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	v := s.entries[request.Key]

	if (v == nil && request.PreviousVersion != 0) || (v != nil && v.Version != request.PreviousVersion) || (v != nil && !valuesEqual(v.Value, request.PreviousValue)) {
		return &api.ReplaceResponse{
			Header: header,
			Status: api.ResponseStatus_PRECONDITION_FAILED,
		}, nil
	}

	if v != nil && valuesEqual(v.Value, request.NewValue) {
		return &api.ReplaceResponse{
			Header: header,
			Status: api.ResponseStatus_NOOP,
		}, nil
	}

	s.entries[request.Key] = &KeyValue{
		Key:     request.Key,
		Value:   request.NewValue,
		Version: int64(index),
	}

	for _, stream := range session.Streams() {
		if v.Version == 0 {
			stream.Send(&api.EventResponse{
				Header:     stream.NewResponseHeader(),
				Type:       api.EventResponse_INSERTED,
				Key:        request.Key,
				NewValue:   request.NewValue,
				NewVersion: int64(index),
			})
		} else {
			stream.Send(&api.EventResponse{
				Header:     stream.NewResponseHeader(),
				Type:       api.EventResponse_UPDATED,
				Key:        request.Key,
				OldValue:   v.Value,
				OldVersion: v.Version,
				NewValue:   request.NewValue,
				NewVersion: int64(index),
			})
		}
	}

	if v != nil {
		return &api.ReplaceResponse{
			Header:          header,
			Status:          api.ResponseStatus_OK,
			PreviousValue:   v.Value,
			PreviousVersion: v.Version,
		}, nil
	}
	return &api.ReplaceResponse{
		Header: header,
		Status: api.ResponseStatus_OK,
	}, nil
}

func (s *TestServer) Exists(ctx context.Context, request *api.ExistsRequest) (*api.ExistsResponse, error) {
	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return nil, err
	}

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	_, ok := s.entries[request.Key]
	if !ok {
		return &api.ExistsResponse{
			Header:      header,
			ContainsKey: false,
		}, nil
	}
	return &api.ExistsResponse{
		Header:      header,
		ContainsKey: true,
	}, nil
}

func (s *TestServer) Size(ctx context.Context, request *api.SizeRequest) (*api.SizeResponse, error) {
	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return nil, err
	}

	headers, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}
	return &api.SizeResponse{
		Header: headers,
		Size_:  int32(len(s.entries)),
	}, nil
}

func (s *TestServer) Clear(ctx context.Context, request *api.ClearRequest) (*api.ClearResponse, error) {
	s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.RequestID
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	headers, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	s.entries = make(map[string]*KeyValue)

	return &api.ClearResponse{
		Header: headers,
	}, nil
}

func (s *TestServer) Events(request *api.EventRequest, server api.MapService_EventsServer) error {
	s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return err
	}

	sequenceNumber := request.Header.RequestID
	session.Await(sequenceNumber)
	session.Complete(sequenceNumber)

	c := make(chan interface{})
	stream := session.NewStream(c)

	for e := range c {
		if err := server.Send(e.(*api.EventResponse)); err != nil {
			if err == io.EOF {
				stream.Delete()
				return nil
			}
			return err
		}
	}
	return nil
}

func (s *TestServer) Entries(request *api.EntriesRequest, server api.MapService_EntriesServer) error {
	return errors.New("not implemented")
}

func valuesEqual(a []byte, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMapOperations(t *testing.T) {
	conn, server := test.StartTestServer(func(server *grpc.Server) {
		api.RegisterMapServiceServer(server, NewTestServer())
	})

	m, err := newPartition(context.TODO(), conn, primitive.NewName("default", "test", "default", "test"))
	assert.NoError(t, err)

	kv, err := m.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.Nil(t, kv)

	size, err := m.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = m.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	kv, err = m.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))
	version := kv.Version

	size, err = m.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	kv, err = m.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))
	assert.Equal(t, version, kv.Version)

	size, err = m.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = m.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	kv, err = m.Put(context.Background(), "bar", []byte("baz"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", string(kv.Value))

	kv, err = m.Put(context.Background(), "foo", []byte("baz"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", string(kv.Value))

	err = m.Clear(context.Background())
	assert.NoError(t, err)

	size, err = m.Len(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = m.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv1, err := m.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	_, err = m.Put(context.Background(), "foo", []byte("baz"), WithVersion(1))
	assert.Error(t, err)

	kv2, err := m.Put(context.Background(), "foo", []byte("baz"), WithVersion(kv1.Version))
	assert.NoError(t, err)
	assert.NotEqual(t, kv1.Version, kv2.Version)
	assert.Equal(t, "baz", string(kv2.Value))

	_, err = m.Remove(context.Background(), "foo", WithVersion(1))
	assert.Error(t, err)

	removed, err := m.Remove(context.Background(), "foo", WithVersion(kv2.Version))
	assert.NoError(t, err)
	assert.NotNil(t, removed)
	assert.Equal(t, kv2.Version, removed.Version)

	test.StopTestServer(server)
}

func TestMapStreams(t *testing.T) {
	conn, server := test.StartTestServer(func(server *grpc.Server) {
		api.RegisterMapServiceServer(server, NewTestServer())
	})

	m, err := newPartition(context.TODO(), conn, primitive.NewName("default", "test", "default", "test"))
	assert.NoError(t, err)

	kv, err := m.Put(context.Background(), "foo", []byte{1})
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	c := make(chan *Event)
	latch := make(chan struct{})
	go func() {
		e := <-c
		assert.Equal(t, "foo", e.Key)
		assert.Equal(t, byte(2), e.Value[0])
		e = <-c
		assert.Equal(t, "bar", e.Key)
		assert.Equal(t, byte(3), e.Value[0])
		e = <-c
		assert.Equal(t, "baz", e.Key)
		assert.Equal(t, byte(4), e.Value[0])
		e = <-c
		assert.Equal(t, "foo", e.Key)
		assert.Equal(t, byte(5), e.Value[0])
		latch <- struct{}{}
	}()

	err = m.Watch(context.Background(), c)
	assert.NoError(t, err)

	kv, err = m.Put(context.Background(), "foo", []byte{2})
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, byte(2), kv.Value[0])

	kv, err = m.Put(context.Background(), "bar", []byte{3})
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", kv.Key)
	assert.Equal(t, byte(3), kv.Value[0])

	kv, err = m.Put(context.Background(), "baz", []byte{4})
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "baz", kv.Key)
	assert.Equal(t, byte(4), kv.Value[0])

	kv, err = m.Put(context.Background(), "foo", []byte{5})
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, byte(5), kv.Value[0])

	<-latch

	test.StopTestServer(server)
}
