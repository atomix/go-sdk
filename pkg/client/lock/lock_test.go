package lock

import (
	"context"
	"errors"
	"github.com/atomix/atomix-go/pkg/client/protocol"
	"github.com/atomix/atomix-go/proto/headers"
	pb "github.com/atomix/atomix-go/proto/lock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

// NewTestServer creates a new server for managing sessions
func NewTestServer() *TestServer {
	return &TestServer{
		sessions: make(map[uint64]*TestSession),
		index:    0,
		queue: []*LockAttempt{},
	}
}

type LockAttempt struct {
	version uint64
	time time.Time
	request *pb.LockRequest
	c chan<-bool
}

// TestServer manages the map state machine for testing
type TestServer struct {
	sessions map[uint64]*TestSession
	index    uint64
	lock *LockAttempt
	queue []*LockAttempt
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
func (s *TestSession) Send(event *pb.LockResponse) {
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
	s.getLatch(sequence + 1) <- struct{}{}
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
	c        chan<- *pb.LockResponse
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
func (s *TestStream) Send(response *pb.LockResponse) {
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

func (s *TestServer) Lock(ctx context.Context, request *pb.LockRequest) (*pb.LockResponse, error) {
	index := s.incrementIndex()

	session, ok := s.sessions[request.Headers.SessionId]
	if !ok {
		return nil, errors.New("session not found")
	}

	sequenceNumber := request.Headers.Headers[0].SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	if s.lock != nil {
		c := make(chan bool)
		attempt := &LockAttempt{
			request: request,
			c: c,
			time: time.Now(),
		}
		s.queue = append(s.queue, attempt)
		succeeded := <-c

		headers, err := session.newResponseHeaders()
		if err != nil {
			return nil, err
		}

		if succeeded {
			attempt.version = s.index
			s.lock = attempt
			return &pb.LockResponse{
				Headers: headers,
				Version: s.index,
			}, nil
		} else {
			return &pb.LockResponse{
				Headers: headers,
				Version: 0,
			}, nil
		}
	} else {
		headers, err := session.newResponseHeaders()
		if err != nil {
			return nil, err
		}

		s.lock = &LockAttempt{
			version: index,
			request: request,
		}
		return &pb.LockResponse{
			Headers: headers,
			Version: index,
		}, nil
	}
}

func (s *TestServer) Unlock(ctx context.Context, request *pb.UnlockRequest) (*pb.UnlockResponse, error) {
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

	if s.lock == nil || (request.Version != 0 && s.lock.version != request.Version) {
		return &pb.UnlockResponse{
			Headers: headers,
			Unlocked: false,
		}, nil
	}

	s.lock = nil

	if len(s.queue) > 0 {
		attempt := s.queue[0]
		s.queue = s.queue[1:]

		if attempt.request.Timeout != nil && (attempt.request.Timeout.Seconds > 0 || attempt.request.Timeout.Nanos > 0) {
			t := time.Duration(attempt.request.Timeout.Seconds + int64(attempt.request.Timeout.Nanos))
			d := time.Now().Sub(attempt.time)
			if d > t {
				attempt.c<-false
			} else {
				attempt.c<-true
			}
		} else {
			attempt.c <- true
		}
	}

	return &pb.UnlockResponse{
		Headers: headers,
		Unlocked: true,
	}, nil
}

func (s *TestServer) IsLocked(ctx context.Context, request *pb.IsLockedRequest) (*pb.IsLockedResponse, error) {
	session, ok := s.sessions[request.Headers.SessionId]
	if !ok {
		return nil, errors.New("session not found")
	}

	headers, err := session.newResponseHeaders()
	if err != nil {
		return nil, err
	}

	if s.lock == nil {
		return &pb.IsLockedResponse{
			Headers: headers,
			IsLocked: false,
		}, nil
	} else if request.Version > 0 && s.lock.version != request.Version {
		return &pb.IsLockedResponse{
			Headers: headers,
			IsLocked: false,
		}, nil
	} else {
		return &pb.IsLockedResponse{
			Headers: headers,
			IsLocked: true,
		}, nil
	}
}

func serve(l *bufconn.Listener, c <-chan struct{}) {
	s := grpc.NewServer()
	pb.RegisterLockServiceServer(s, NewTestServer())
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

func TestLock(t *testing.T) {
	conn, test := startTest()

	l1, err := NewLock(conn, "test", protocol.MultiRaft("test"))
	assert.NoError(t, err)

	l2, err := NewLock(conn, "test", protocol.MultiRaft("test"))
	assert.NoError(t, err)

	v1, err := l1.Lock(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, 0, v1)

	locked, err := l1.IsLocked(context.Background())
	assert.NoError(t, err)
	assert.True(t, locked)

	locked, err = l2.IsLocked(context.Background())
	assert.NoError(t, err)
	assert.True(t, locked)

	var v2 uint64
	c := make(chan struct{})
	go func() {
		v2, err = l2.Lock(context.Background())
		assert.NoError(t, err)
		c<-struct{}{}
	}()

	success, err := l1.Unlock(context.Background())
	assert.NoError(t, err)
	assert.True(t, success)

	<-c

	assert.NotEqual(t, v1, v2)

	locked, err = l1.IsLocked(context.Background())
	assert.NoError(t, err)
	assert.True(t, locked)

	locked, err = l1.IsLocked(context.Background(), IsLockedVersion(v1))
	assert.NoError(t, err)
	assert.False(t, locked)

	locked, err = l1.IsLocked(context.Background(), IsLockedVersion(v2))
	assert.NoError(t, err)
	assert.True(t, locked)

	stopTest(test)
}
