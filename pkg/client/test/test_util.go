package test

import (
	"context"
	"errors"
	"github.com/atomix/atomix-go-client/proto/atomix/headers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
	"sync"
)

func NewTestServer() *TestServer {
	return &TestServer{
		sessions: make(map[uint64]*TestSession),
		Index:    0,
	}
}

// TestServer manages the map state machine for testing
type TestServer struct {
	sessions map[uint64]*TestSession
	Index    uint64
}

// IncrementIndex increments and returns the server's index
func (s *TestServer) IncrementIndex() uint64 {
	s.Index += 1
	return s.Index
}

func (s *TestServer) CreateHeader(ctx context.Context) (*headers.ResponseHeader, error) {
	index := s.IncrementIndex()
	session := s.NewSession()
	session.Complete(0)
	return &headers.ResponseHeader{
		SessionId: index,
		Index:     index,
	}, nil
}

func (s *TestServer) KeepAliveHeader(ctx context.Context, h *headers.RequestHeader) (*headers.ResponseHeader, error) {
	index := s.IncrementIndex()
	if session, exists := s.sessions[h.SessionId]; exists {
		return &headers.ResponseHeader{
			SessionId:  h.SessionId,
			Index:      index,
			ResponseId: session.SequenceNumber,
		}, nil
	} else {
		return nil, errors.New("session not found")
	}
}

func (s *TestServer) CloseHeader(ctx context.Context, h *headers.RequestHeader) error {
	s.IncrementIndex()
	if _, exists := s.sessions[h.SessionId]; exists {
		delete(s.sessions, h.SessionId)
		return nil
	} else {
		return errors.New("session not found")
	}
}

// NewSession adds a test session to the server
func (s *TestServer) NewSession() *TestSession {
	session := &TestSession{
		Id:             s.Index,
		server:         s,
		sequences:      make(map[uint64]chan struct{}),
		mu:             sync.Mutex{},
		SequenceNumber: 0,
		streams:        make(map[uint64]*TestStream),
	}
	s.sessions[session.Id] = session
	return session
}

// GetSession gets a test session
func (s *TestServer) GetSession(id uint64) (*TestSession, error) {
	sess, ok := s.sessions[id]
	if ok {
		return sess, nil
	} else {
		return sess, errors.New("session not found")
	}
}

// TestSession manages a session, orders session operations, and manages streams for the session
type TestSession struct {
	Id             uint64
	server         *TestServer
	sequences      map[uint64]chan struct{}
	mu             sync.Mutex
	SequenceNumber uint64
	streams        map[uint64]*TestStream
}

// NewStream creates a new stream for the session
func (s *TestSession) NewStream(c chan<- interface{}) *TestStream {
	stream := &TestStream{
		Id:         s.server.Index,
		session:    s,
		ItemNumber: 0,
		c:          c,
	}
	s.streams[stream.Id] = stream
	return stream
}

// Streams returns a slice of all streams open for the sesssion
func (s *TestSession) Streams() []*TestStream {
	streams := make([]*TestStream, 0)
	for _, value := range s.streams {
		streams = append(streams, value)
	}
	return streams
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
	s.SequenceNumber = sequence
}

// Complete unblocks the command following the given sequence number to be applied to the server
func (s *TestSession) Complete(sequence uint64) {
	s.getLatch(sequence + 1) <- struct{}{}
}

// NewResponseHeaders creates a new response header with headers for all open streams
func (s *TestSession) NewResponseHeader() (*headers.ResponseHeader, error) {
	return &headers.ResponseHeader{
		SessionId:  s.Id,
		Index:      s.server.Index,
		ResponseId: s.SequenceNumber,
	}, nil
}

// Delete deletes the test stream
func (s *TestSession) Delete() {
	delete(s.server.sessions, s.Id)
}

// TestStream manages ordering for a single stream
type TestStream struct {
	session    *TestSession
	Id         uint64
	ItemNumber uint64
	c          chan<- interface{}
}

// header creates a new stream header
func (s *TestStream) Header(index uint64) *headers.StreamHeader {
	return &headers.StreamHeader{
		StreamId:   s.Id,
		ResponseId: s.ItemNumber,
	}
}

// NewResponseHeaders returns headers for the stream
func (s *TestStream) NewResponseHeader() *headers.ResponseHeader {
	return &headers.ResponseHeader{
		SessionId:  s.Id,
		Index:      s.session.server.Index,
		ResponseId: s.session.SequenceNumber,
	}
}

// Send sends an EventResponse on the stream
func (s *TestStream) Send(response interface{}) {
	s.ItemNumber += 1
	s.c <- response
}

// Delete deletes the test stream
func (s *TestStream) Delete() {
	delete(s.session.streams, s.Id)
}

func serve(r func(server *grpc.Server), l *bufconn.Listener, c <-chan struct{}) {
	s := grpc.NewServer()
	r(s)
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

func StartTestServer(r func(server *grpc.Server)) (*grpc.ClientConn, chan struct{}) {
	l := bufconn.Listen(1024 * 1024)
	stop := make(chan struct{})
	serve(r, l, stop)

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

func StopTestServer(c chan struct{}) {
	c <- struct{}{}
}
