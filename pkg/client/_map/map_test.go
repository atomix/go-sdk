package _map

import (
	"context"
	"errors"
	"github.com/atomix/atomix-go-client/pkg/client/test"
	pb "github.com/atomix/atomix-go-client/proto/atomix/map"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"io"
	"testing"
)

// NewTestServer creates a new server for managing sessions
func NewTestServer() *TestServer {
	return &TestServer{
		TestServer: test.NewTestServer(),
		entries:    make(map[string]*KeyValue),
	}
}

type TestServer struct {
	*test.TestServer
	entries map[string]*KeyValue
}

func (s *TestServer) Create(ctx context.Context, request *pb.CreateRequest) (*pb.CreateResponse, error) {
	headers, err := s.CreateHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.CreateResponse{
		Header: headers,
	}, nil
}

func (s *TestServer) KeepAlive(ctx context.Context, request *pb.KeepAliveRequest) (*pb.KeepAliveResponse, error) {
	headers, err := s.KeepAliveHeader(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	return &pb.KeepAliveResponse{
		Header: headers,
	}, nil
}

func (s *TestServer) Close(ctx context.Context, request *pb.CloseRequest) (*pb.CloseResponse, error) {
	err := s.CloseHeader(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	return &pb.CloseResponse{}, nil
}

func (s *TestServer) Put(ctx context.Context, request *pb.PutRequest) (*pb.PutResponse, error) {
	index := s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	v := s.entries[request.Key]

	if request.Version != 0 && (v == nil || v.Version != request.Version) {
		return &pb.PutResponse{
			Header: header,
			Status: pb.ResponseStatus_PRECONDITION_FAILED,
		}, nil
	}

	if v != nil && valuesEqual(v.Value, request.Value) {
		return &pb.PutResponse{
			Header: header,
			Status: pb.ResponseStatus_NOOP,
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
			stream.Send(&pb.EventResponse{
				Header:     stream.NewResponseHeader(),
				Type:       pb.EventResponse_INSERTED,
				Key:        request.Key,
				NewValue:   request.Value,
				NewVersion: request.Version,
			})
		} else {
			stream.Send(&pb.EventResponse{
				Header:     stream.NewResponseHeader(),
				Type:       pb.EventResponse_UPDATED,
				Key:        request.Key,
				OldValue:   v.Value,
				OldVersion: v.Version,
				NewValue:   request.Value,
				NewVersion: request.Version,
			})
		}
	}

	if v != nil {
		return &pb.PutResponse{
			Header:          header,
			Status:          pb.ResponseStatus_OK,
			PreviousValue:   v.Value,
			PreviousVersion: v.Version,
		}, nil
	} else {
		return &pb.PutResponse{
			Header: header,
			Status: pb.ResponseStatus_OK,
		}, nil
	}
}

func (s *TestServer) Get(ctx context.Context, request *pb.GetRequest) (*pb.GetResponse, error) {
	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return nil, err
	}

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	v := s.entries[request.Key]

	if v.Version != 0 {
		return &pb.GetResponse{
			Header:  header,
			Value:   v.Value,
			Version: v.Version,
		}, nil
	} else {
		return &pb.GetResponse{
			Header: header,
		}, nil
	}
}

func (s *TestServer) Remove(ctx context.Context, request *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	v := s.entries[request.Key]

	if v == nil {
		return &pb.RemoveResponse{
			Header: header,
			Status: pb.ResponseStatus_NOOP,
		}, nil
	}

	if request.Version != 0 && v.Version != request.Version {
		return &pb.RemoveResponse{
			Header: header,
			Status: pb.ResponseStatus_PRECONDITION_FAILED,
		}, nil
	}

	delete(s.entries, request.Key)

	for _, stream := range session.Streams() {
		stream.Send(&pb.EventResponse{
			Header:     stream.NewResponseHeader(),
			Type:       pb.EventResponse_REMOVED,
			Key:        request.Key,
			OldValue:   v.Value,
			OldVersion: v.Version,
		})
	}

	if v.Version != 0 {
		return &pb.RemoveResponse{
			Header:          header,
			Status:          pb.ResponseStatus_OK,
			PreviousValue:   v.Value,
			PreviousVersion: v.Version,
		}, nil
	} else {
		return &pb.RemoveResponse{
			Header: header,
			Status: pb.ResponseStatus_OK,
		}, nil
	}
}

func (s *TestServer) Replace(ctx context.Context, request *pb.ReplaceRequest) (*pb.ReplaceResponse, error) {
	index := s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	v := s.entries[request.Key]

	if (v == nil && request.PreviousVersion != 0) || (v != nil && v.Version != request.PreviousVersion) || (v != nil && !valuesEqual(v.Value, request.PreviousValue)) {
		return &pb.ReplaceResponse{
			Header: header,
			Status: pb.ResponseStatus_PRECONDITION_FAILED,
		}, nil
	}

	if v != nil && valuesEqual(v.Value, request.NewValue) {
		return &pb.ReplaceResponse{
			Header: header,
			Status: pb.ResponseStatus_NOOP,
		}, nil
	}

	s.entries[request.Key] = &KeyValue{
		Key:     request.Key,
		Value:   request.NewValue,
		Version: int64(index),
	}

	for _, stream := range session.Streams() {
		if v.Version == 0 {
			stream.Send(&pb.EventResponse{
				Header:     stream.NewResponseHeader(),
				Type:       pb.EventResponse_INSERTED,
				Key:        request.Key,
				NewValue:   request.NewValue,
				NewVersion: int64(index),
			})
		} else {
			stream.Send(&pb.EventResponse{
				Header:     stream.NewResponseHeader(),
				Type:       pb.EventResponse_UPDATED,
				Key:        request.Key,
				OldValue:   v.Value,
				OldVersion: v.Version,
				NewValue:   request.NewValue,
				NewVersion: int64(index),
			})
		}
	}

	if v != nil {
		return &pb.ReplaceResponse{
			Header:          header,
			Status:          pb.ResponseStatus_OK,
			PreviousValue:   v.Value,
			PreviousVersion: v.Version,
		}, nil
	} else {
		return &pb.ReplaceResponse{
			Header: header,
			Status: pb.ResponseStatus_OK,
		}, nil
	}
}

func (s *TestServer) Exists(ctx context.Context, request *pb.ExistsRequest) (*pb.ExistsResponse, error) {
	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return nil, err
	}

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	_, ok := s.entries[request.Key]
	if !ok {
		return &pb.ExistsResponse{
			Header:      header,
			ContainsKey: false,
		}, nil
	} else {
		return &pb.ExistsResponse{
			Header:      header,
			ContainsKey: true,
		}, nil
	}
}

func (s *TestServer) Size(ctx context.Context, request *pb.SizeRequest) (*pb.SizeResponse, error) {
	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return nil, err
	}

	headers, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}
	return &pb.SizeResponse{
		Header: headers,
		Size:   int32(len(s.entries)),
	}, nil
}

func (s *TestServer) Clear(ctx context.Context, request *pb.ClearRequest) (*pb.ClearResponse, error) {
	s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	headers, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	s.entries = make(map[string]*KeyValue)

	return &pb.ClearResponse{
		Header: headers,
	}, nil
}

func (s *TestServer) Events(request *pb.EventRequest, server pb.MapService_EventsServer) error {
	s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return err
	}

	sequenceNumber := request.Header.SequenceNumber
	session.Await(sequenceNumber)
	session.Complete(sequenceNumber)

	c := make(chan interface{})
	stream := session.NewStream(c)

	for e := range c {
		if err := server.Send(e.(*pb.EventResponse)); err != nil {
			if err == io.EOF {
				stream.Delete()
				return nil
			}
			return err
		}
	}
	return nil
}

func (s *TestServer) Entries(request *pb.EntriesRequest, server pb.MapService_EntriesServer) error {
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
		pb.RegisterMapServiceServer(server, NewTestServer())
	})

	m, err := newPartition(context.TODO(), conn, "test", "test")
	assert.NoError(t, err)

	size, err := m.Size(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err := m.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "bar", string(kv.Value))

	kv, err = m.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))
	version := kv.Version

	size, err = m.Size(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	kv, err = m.Remove(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, "foo", kv.Key)
	assert.Equal(t, "bar", string(kv.Value))
	assert.Equal(t, version, kv.Version)

	size, err = m.Size(context.Background())
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

	size, err = m.Size(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	kv, err = m.Put(context.Background(), "foo", []byte("bar"))
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv1, err := m.Get(context.Background(), "foo")
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	kv, err = m.Put(context.Background(), "foo", []byte("baz"), WithVersion(1))
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
		pb.RegisterMapServiceServer(server, NewTestServer())
	})

	m, err := newPartition(context.TODO(), conn, "test", "test")
	assert.NoError(t, err)

	kv, err := m.Put(context.Background(), "foo", []byte{1})
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	c := make(chan *MapEvent)
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

	err = m.Listen(context.Background(), c)
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
