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
	"io"
	"log"
	"net"
	"sync"
	"testing"
)

// NewTestServer creates a new server for managing sessions
func NewTestServer() *TestServer {
	return &TestServer{
		sessions: make(map[uint64]*TestSession),
		index:    0,
		entries:  make(map[string]*KeyValue),
	}
}

// TestServer manages the map state machine for testing
type TestServer struct {
	sessions map[uint64]*TestSession
	index    uint64
	entries  map[string]*KeyValue
	mu       sync.Mutex
	queue    chan uint64
}

// TestSession manages a session, orders session operations, and manages streams for the session
type TestSession struct {
	id        uint64
	server    *TestServer
	sequences map[uint64]chan struct{}
	mu        sync.Mutex
	sequence  uint64
	streams   map[uint64]*TestStream
}

// Send sends an event response to all open streams
func (s *TestSession) Send(event *pb.EventResponse) {
	for _, stream := range s.streams {
		stream.Send(event)
	}
}

// getLatch returns a channel on which to wait for operations to be ordered for the given sequence number
func (s *TestSession) getLatch(sequence uint64) chan struct{} {
	s.mu.Lock()
	if _, ok := s.sequences[sequence]; !ok {
		s.sequences[sequence] = make(chan struct{}, 1)
	}
	latch := s.sequences[sequence]
	s.mu.Unlock()
	return latch
}

// Await waits for all commands prior to the given sequence number to be applied to the server
func (s *TestSession) Await(sequence uint64) {
	<-s.getLatch(sequence)
	s.sequence = sequence
}

// Complete unblocks the command following the given sequence number to be applied to the server
func (s *TestSession) Complete(sequence uint64) {
	s.getLatch(sequence + 1)<-struct{}{}
}

// newResponseHeader creates a new response header with headers for all open streams
func (s *TestSession) newResponseHeaders() (*headers.SessionResponseHeaders, error) {
	streams := []*headers.SessionStreamHeader{}
	for _, stream := range s.streams {
		streams = append(streams, stream.header(uint64(s.server.index)))
	}
	return &headers.SessionResponseHeaders{
		SessionId: s.id,
		Headers: []*headers.SessionResponseHeader{
			{
				PartitionId:    1,
				Index:          s.server.index,
				SequenceNumber: s.sequence,
				Streams:        streams,
			},
		},
	}, nil
}

// TestStream manages ordering for a single stream
type TestStream struct {
	server   *TestServer
	id       uint64
	sequence uint64
	c        chan<- *pb.EventResponse
}

// header creates a new stream header
func (s *TestStream) header(index uint64) *headers.SessionStreamHeader {
	return &headers.SessionStreamHeader{
		StreamId:       s.id,
		Index:          index,
		LastItemNumber: s.sequence,
	}
}

// Send sends an EventResponse on the stream
func (s *TestStream) Send(response *pb.EventResponse) {
	s.sequence += 1
	response.Headers.Headers[0].Streams = []*headers.SessionStreamHeader{
		{
			StreamId:       s.id,
			Index:          s.server.index,
			LastItemNumber: s.sequence,
		},
	}
	s.c <- response
}

// incrementIndex increments and returns the server's index
func (s *TestServer) incrementIndex() uint64 {
	s.index += 1
	return s.index
}

func (s *TestServer) Create(ctx context.Context, request *pb.CreateRequest) (*pb.CreateResponse, error) {
	index := s.incrementIndex()
	session := &TestSession{
		id:        index,
		server:    s,
		sequences: make(map[uint64]chan struct{}),
		streams:   make(map[uint64]*TestStream),
		mu:        sync.Mutex{},
	}
	s.sessions[index] = session
	session.Complete(0)
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
	if session, exists := s.sessions[request.Headers.SessionId]; exists {
		streams := []*headers.SessionStreamHeader{}
		for _, stream := range session.streams {
			streams = append(streams, stream.header(uint64(index)))
		}
		return &pb.KeepAliveResponse{
			Headers: &headers.SessionHeaders{
				SessionId: request.Headers.SessionId,
				Headers: []*headers.SessionHeader{
					{
						PartitionId:        1,
						LastSequenceNumber: session.sequence,
						Streams:            streams,
					},
				},
			},
		}, nil
	} else {
		return nil, errors.New("session not found")
	}
}

func (s *TestServer) Close(ctx context.Context, request *pb.CloseRequest) (*pb.CloseResponse, error) {
	s.incrementIndex()
	if _, exists := s.sessions[request.Headers.SessionId]; exists {
		return &pb.CloseResponse{}, nil
	} else {
		return nil, errors.New("session not found")
	}
}

func (s *TestServer) Put(ctx context.Context, request *pb.PutRequest) (*pb.PutResponse, error) {
	index := s.incrementIndex()

	session, ok := s.sessions[request.Headers.SessionId]
	if !ok {
		return nil, errors.New("session not found")
	}

	sequenceNumber := request.Headers.Headers[0].SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	headers, err := session.newResponseHeaders()
	if err != nil {
		return nil, err
	}

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

	if v == nil {
		session.Send(&pb.EventResponse{
			Headers:    headers,
			Type:       pb.EventResponse_INSERTED,
			Key:        request.Key,
			NewValue:   request.Value,
			NewVersion: request.Version,
		})
	} else {
		session.Send(&pb.EventResponse{
			Headers:    headers,
			Type:       pb.EventResponse_UPDATED,
			Key:        request.Key,
			OldValue:   v.Value,
			OldVersion: v.Version,
			NewValue:   request.Value,
			NewVersion: request.Version,
		})
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
	session, ok := s.sessions[request.Headers.SessionId]
	if !ok {
		return nil, errors.New("session not found")
	}

	headers, err := session.newResponseHeaders()
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

	session, ok := s.sessions[request.Headers.SessionId]
	if !ok {
		return nil, errors.New("session not found")
	}

	sequenceNumber := request.Headers.Headers[0].SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	headers, err := session.newResponseHeaders()
	if err != nil {
		return nil, err
	}

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

	session.Send(&pb.EventResponse{
		Headers:    headers,
		Type:       pb.EventResponse_REMOVED,
		Key:        request.Key,
		OldValue:   v.Value,
		OldVersion: v.Version,
	})

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

	session, ok := s.sessions[request.Headers.SessionId]
	if !ok {
		return nil, errors.New("session not found")
	}

	sequenceNumber := request.Headers.Headers[0].SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	headers, err := session.newResponseHeaders()
	if err != nil {
		return nil, err
	}

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

	if v.Version == 0 {
		session.Send(&pb.EventResponse{
			Headers:    headers,
			Type:       pb.EventResponse_INSERTED,
			Key:        request.Key,
			NewValue:   request.NewValue,
			NewVersion: int64(index),
		})
	} else {
		session.Send(&pb.EventResponse{
			Headers:    headers,
			Type:       pb.EventResponse_UPDATED,
			Key:        request.Key,
			OldValue:   v.Value,
			OldVersion: v.Version,
			NewValue:   request.NewValue,
			NewVersion: int64(index),
		})
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
	session, ok := s.sessions[request.Headers.SessionId]
	if !ok {
		return nil, errors.New("session not found")
	}

	headers, err := session.newResponseHeaders()
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
	session, ok := s.sessions[request.Headers.SessionId]
	if !ok {
		return nil, errors.New("session not found")
	}

	headers, err := session.newResponseHeaders()
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

	session, ok := s.sessions[request.Headers.SessionId]
	if !ok {
		return nil, errors.New("session not found")
	}

	sequenceNumber := request.Headers.Headers[0].SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	headers, err := session.newResponseHeaders()
	if err != nil {
		return nil, err
	}

	s.entries = make(map[string]*KeyValue)

	return &pb.ClearResponse{
		Headers: headers,
	}, nil
}

func (s *TestServer) Events(request *pb.EventRequest, server pb.MapService_EventsServer) error {
	index := s.incrementIndex()

	session, ok := s.sessions[request.Headers.SessionId]
	if !ok {
		return errors.New("session not found")
	}

	sequenceNumber := request.Headers.Headers[0].SequenceNumber
	session.Await(sequenceNumber)
	session.Complete(sequenceNumber)

	c := make(chan *pb.EventResponse)
	session.streams[index] = &TestStream{
		server: s,
		id:     index,
		c:      c,
	}

	for {
		if err := server.Send(<-c); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
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

func startTest() (*grpc.ClientConn, chan struct{}) {
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

	go func() {
		<-stop
		c.Close()
	}()

	return c, stop
}

func stopTest(c chan struct{}) {
	c <- struct{}{}
}

func TestMapOperations(t *testing.T) {
	conn, test := startTest()

	m, err := NewMap(conn, "test", protocol.MultiRaft("test"))
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

	stopTest(test)
}

func TestMapStreams(t *testing.T) {
	conn, test := startTest()

	m, err := NewMap(conn, "test", protocol.MultiRaft("test"))
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

	stopTest(test)
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
