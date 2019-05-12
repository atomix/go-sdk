package _map

import (
	"context"
	"errors"
	"github.com/atomix/atomix-go/pkg/client/protocol"
	"github.com/atomix/atomix-go/proto/headers"
	pb "github.com/atomix/atomix-go/proto/map"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
	"testing"
)

func NewTestServer() *TestServer {
	return &TestServer{
		sessions: make(map[uint64]uint64),
		index:    0,
		entries:  make(map[string]*KeyValue),
		streams:  make(map[uint64]*TestStream),
	}
}

type TestServer struct {
	sessions map[uint64]uint64
	index    uint64
	entries  map[string]*KeyValue
	streams  map[uint64]*TestStream
}

type TestStream struct {
	server   *TestServer
	id       uint64
	stream   pb.MapService_EventsServer
	sequence uint64
}

func (s *TestStream) header(index uint64) *headers.SessionStreamHeader {
	return &headers.SessionStreamHeader{
		StreamId:       s.id,
		Index:          index,
		LastItemNumber: s.sequence,
	}
}

func (s *TestStream) Send(response *pb.EventResponse) error {
	s.sequence += 1
	response.Headers.Headers[0].Streams = []*headers.SessionStreamHeader{
		{
			StreamId:       s.id,
			Index:          s.server.index,
			LastItemNumber: s.sequence,
		},
	}
	return s.stream.Send(response)
}

func (s *TestServer) incrementIndex() uint64 {
	s.index += 1
	return s.index
}

func (s *TestServer) Create(ctx context.Context, request *pb.CreateRequest) (*pb.CreateResponse, error) {
	index := s.incrementIndex()
	s.sessions[index] = 0
	return &pb.CreateResponse{
		Headers: &headers.SessionHeaders{
			SessionId: index,
			Headers: []*headers.SessionHeader{
				{
					PartitionId: 1,
				},
			},
		},
	}, nil
}

func (s *TestServer) KeepAlive(ctx context.Context, request *pb.KeepAliveRequest) (*pb.KeepAliveResponse, error) {
	index := s.incrementIndex()
	if sequence, exists := s.sessions[request.Headers.SessionId]; exists {
		streams := []*headers.SessionStreamHeader{}
		for _, stream := range s.streams {
			streams = append(streams, stream.header(uint64(index)))
		}
		return &pb.KeepAliveResponse{
			Headers: &headers.SessionHeaders{
				SessionId: request.Headers.SessionId,
				Headers: []*headers.SessionHeader{
					{
						PartitionId:        1,
						LastSequenceNumber: sequence,
						Streams:            streams,
					},
				},
			},
		}, nil
	} else {
		return nil, errors.New("session does not exist")
	}
}

func (s *TestServer) Close(ctx context.Context, request *pb.CloseRequest) (*pb.CloseResponse, error) {
	s.incrementIndex()
	if _, exists := s.sessions[request.Headers.SessionId]; exists {
		return &pb.CloseResponse{}, nil
	} else {
		return nil, errors.New("session does not exist")
	}
}

func (s *TestServer) Put(ctx context.Context, request *pb.PutRequest) (*pb.PutResponse, error) {
	index := s.incrementIndex()

	headers, err := s.newResponseHeaders(request.Headers.SessionId)
	if err != nil {
		return nil, err
	}

	sequence := request.Headers.Headers[0].SequenceNumber
	s.sessions[request.Headers.SessionId] = sequence

	v := s.entries[request.Key]

	if request.Version != 0 && (v == nil || v.Version != request.Version) {
		return &pb.PutResponse{
			Headers: headers,
			Status:  pb.ResponseStatus_PRECONDITION_FAILED,
		}, nil
	}

	if v != nil && valuesEqual(v.Value, request.Value) {
		return &pb.PutResponse{
			Headers: headers,
			Status:  pb.ResponseStatus_NOOP,
		}, nil
	}

	s.entries[request.Key] = &KeyValue{
		Key:     request.Key,
		Value:   request.Value,
		Version: int64(index),
	}

	for _, stream := range s.streams {
		if v.Version == 0 {
			stream.Send(&pb.EventResponse{
				Headers:    headers,
				Type:       pb.EventResponse_INSERTED,
				Key:        request.Key,
				NewValue:   request.Value,
				NewVersion: request.Version,
			})
		} else {
			stream.Send(&pb.EventResponse{
				Headers:    headers,
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
			Headers:         headers,
			Status:          pb.ResponseStatus_OK,
			PreviousValue:   v.Value,
			PreviousVersion: v.Version,
		}, nil
	} else {
		return &pb.PutResponse{
			Headers: headers,
			Status:  pb.ResponseStatus_OK,
		}, nil
	}
}

func (s *TestServer) Get(ctx context.Context, request *pb.GetRequest) (*pb.GetResponse, error) {
	headers, err := s.newResponseHeaders(request.Headers.SessionId)
	if err != nil {
		return nil, err
	}

	v := s.entries[request.Key]

	if v.Version != 0 {
		return &pb.GetResponse{
			Headers: headers,
			Value:   v.Value,
			Version: v.Version,
		}, nil
	} else {
		return &pb.GetResponse{
			Headers: headers,
		}, nil
	}
}

func (s *TestServer) Remove(ctx context.Context, request *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	s.incrementIndex()

	headers, err := s.newResponseHeaders(request.Headers.SessionId)
	if err != nil {
		return nil, err
	}

	sequence := request.Headers.Headers[0].SequenceNumber
	s.sessions[request.Headers.SessionId] = sequence

	v := s.entries[request.Key]

	if v == nil {
		return &pb.RemoveResponse{
			Headers: headers,
			Status:  pb.ResponseStatus_NOOP,
		}, nil
	}

	if request.Version != 0 && v.Version != request.Version {
		return &pb.RemoveResponse{
			Headers: headers,
			Status:  pb.ResponseStatus_PRECONDITION_FAILED,
		}, nil
	}

	delete(s.entries, request.Key)

	for _, stream := range s.streams {
		stream.Send(&pb.EventResponse{
			Headers:    headers,
			Type:       pb.EventResponse_REMOVED,
			Key:        request.Key,
			OldValue:   v.Value,
			OldVersion: v.Version,
		})
	}

	if v.Version != 0 {
		return &pb.RemoveResponse{
			Headers:         headers,
			Status:          pb.ResponseStatus_OK,
			PreviousValue:   v.Value,
			PreviousVersion: v.Version,
		}, nil
	} else {
		return &pb.RemoveResponse{
			Headers: headers,
			Status:  pb.ResponseStatus_OK,
		}, nil
	}
}

func (s *TestServer) Replace(ctx context.Context, request *pb.ReplaceRequest) (*pb.ReplaceResponse, error) {
	index := s.incrementIndex()

	headers, err := s.newResponseHeaders(request.Headers.SessionId)
	if err != nil {
		return nil, err
	}

	sequence := request.Headers.Headers[0].SequenceNumber
	s.sessions[request.Headers.SessionId] = sequence

	v := s.entries[request.Key]

	if (v == nil && request.PreviousVersion != 0) || (v != nil && v.Version != request.PreviousVersion) || (v != nil && !valuesEqual(v.Value, request.PreviousValue)) {
		return &pb.ReplaceResponse{
			Headers: headers,
			Status:  pb.ResponseStatus_PRECONDITION_FAILED,
		}, nil
	}

	if v != nil && valuesEqual(v.Value, request.NewValue) {
		return &pb.ReplaceResponse{
			Headers: headers,
			Status:  pb.ResponseStatus_NOOP,
		}, nil
	}

	s.entries[request.Key] = &KeyValue{
		Key:     request.Key,
		Value:   request.NewValue,
		Version: int64(index),
	}

	for _, stream := range s.streams {
		if v.Version == 0 {
			stream.Send(&pb.EventResponse{
				Headers:    headers,
				Type:       pb.EventResponse_INSERTED,
				Key:        request.Key,
				NewValue:   request.NewValue,
				NewVersion: int64(index),
			})
		} else {
			stream.Send(&pb.EventResponse{
				Headers:    headers,
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
			Headers:         headers,
			Status:          pb.ResponseStatus_OK,
			PreviousValue:   v.Value,
			PreviousVersion: v.Version,
		}, nil
	} else {
		return &pb.ReplaceResponse{
			Headers: headers,
			Status:  pb.ResponseStatus_OK,
		}, nil
	}
}

func (s *TestServer) Exists(ctx context.Context, request *pb.ExistsRequest) (*pb.ExistsResponse, error) {
	headers, err := s.newResponseHeaders(request.Headers.SessionId)
	if err != nil {
		return nil, err
	}

	for _, key := range request.Keys {
		_, ok := s.entries[key]
		if !ok {
			return &pb.ExistsResponse{
				Headers:     headers,
				ContainsKey: false,
			}, nil
		}
	}
	return &pb.ExistsResponse{
		Headers:     headers,
		ContainsKey: true,
	}, nil
}

func (s *TestServer) Size(ctx context.Context, request *pb.SizeRequest) (*pb.SizeResponse, error) {
	headers, err := s.newResponseHeaders(request.Headers.SessionId)
	if err != nil {
		return nil, err
	}
	return &pb.SizeResponse{
		Headers: headers,
		Size:    int32(len(s.entries)),
	}, nil
}

func (s *TestServer) Clear(ctx context.Context, request *pb.ClearRequest) (*pb.ClearResponse, error) {
	s.incrementIndex()

	headers, err := s.newResponseHeaders(request.Headers.SessionId)
	if err != nil {
		return nil, err
	}

	sequence := request.Headers.Headers[0].SequenceNumber
	s.sessions[request.Headers.SessionId] = sequence

	s.entries = make(map[string]*KeyValue)

	return &pb.ClearResponse{
		Headers: headers,
	}, nil
}

func (s *TestServer) Events(request *pb.EventRequest, server pb.MapService_EventsServer) error {
	index := s.incrementIndex()
	s.streams[index] = &TestStream{
		server: s,
		id:     index,
		stream: server,
	}
	return nil
}

func (s *TestServer) newResponseHeaders(sessionId uint64) (*headers.SessionResponseHeaders, error) {
	if sequence, exists := s.sessions[sessionId]; exists {
		streams := []*headers.SessionStreamHeader{}
		for _, stream := range s.streams {
			streams = append(streams, stream.header(uint64(s.index)))
		}
		return &headers.SessionResponseHeaders{
			SessionId: sessionId,
			Headers: []*headers.SessionResponseHeader{
				{
					PartitionId:    1,
					Index:          s.index,
					SequenceNumber: sequence,
					Streams:        streams,
				},
			},
		}, nil
	} else {
		return nil, errors.New("session does not exist")
	}
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

func serve(l *bufconn.Listener, c <-chan struct{}) {
	s := grpc.NewServer()
	pb.RegisterMapServiceServer(s, NewTestServer())
	go func() {
		if err := s.Serve(l); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	go func() {
		for {
			select {
			case <-c:
				s.Stop()
				return
			}
		}
	}()
}

func TestMapOperations(t *testing.T) {
	l := bufconn.Listen(1024 * 1024)
	stop := make(chan struct{})
	serve(l, stop)

	f := func(c context.Context, s string) (net.Conn, error) {
		return l.Dial()
	}

	c, err := grpc.Dial("test", grpc.WithContextDialer(f), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	m, err := NewMap(c, "test", protocol.MultiRaft("test"))
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

	kv, err = m.Put(context.Background(), "foo", []byte("baz"), PutIfVersion(1))
	assert.Error(t, err)

	kv2, err := m.Put(context.Background(), "foo", []byte("baz"), PutIfVersion(kv1.Version))
	assert.NoError(t, err)
	assert.NotEqual(t, kv1.Version, kv2.Version)
	assert.Equal(t, "baz", string(kv2.Value))

	_, err = m.Remove(context.Background(), "foo", RemoveIfVersion(1))
	assert.Error(t, err)

	removed, err := m.Remove(context.Background(), "foo", RemoveIfVersion(kv2.Version))
	assert.NoError(t, err)
	assert.NotNil(t, removed)
	assert.Equal(t, kv2.Version, removed.Version)

	defer c.Close()
	stop <- struct{}{}
}

func example() {
	client, err := grpc.Dial("localhost", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	m, err := NewMap(client, "foo", protocol.MultiRaft("test"))
	if err != nil {
		panic(err)
	}

	_, err = m.Put(context.TODO(), "foo", []byte("bar"))
	if err != nil {
		panic(err)
	}

	v, err := m.Get(context.TODO(), "foo")

	println("Key is", v.Key)
	println("Value is", string(v.Value))
	println("Version is", v.Version)

	c := make(chan *MapEvent)
	go m.Listen(context.TODO(), c)

	for {
		select {
		case e := <-c:
			println(e.Key)
		}
	}
}
