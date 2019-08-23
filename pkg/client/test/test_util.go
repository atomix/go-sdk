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

package test

import (
	"context"
	"errors"
	"github.com/atomix/atomix-api/proto/atomix/headers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
	"sync"
)

// NewServer creates a new test Server
func NewServer() *Server {
	return &Server{
		sessions: make(map[uint64]*Session),
		Index:    0,
	}
}

// Server manages the map state machine for testing
type Server struct {
	sessions map[uint64]*Session
	Index    uint64
}

// IncrementIndex increments and returns the server's index
func (s *Server) IncrementIndex() uint64 {
	s.Index++
	return s.Index
}

func (s *Server) CreateHeader(ctx context.Context) (*headers.ResponseHeader, error) {
	index := s.IncrementIndex()
	session := s.NewSession()
	session.Complete(0)
	return &headers.ResponseHeader{
		SessionID: index,
		Index:     index,
	}, nil
}

func (s *Server) KeepAliveHeader(ctx context.Context, h *headers.RequestHeader) (*headers.ResponseHeader, error) {
	index := s.IncrementIndex()
	if session, exists := s.sessions[h.SessionID]; exists {
		return &headers.ResponseHeader{
			SessionID:  h.SessionID,
			Index:      index,
			ResponseID: session.SequenceNumber,
		}, nil
	}
	return nil, errors.New("session not found")
}

func (s *Server) CloseHeader(ctx context.Context, h *headers.RequestHeader) error {
	s.IncrementIndex()
	if _, exists := s.sessions[h.SessionID]; exists {
		delete(s.sessions, h.SessionID)
		return nil
	}
	return errors.New("session not found")
}

// NewSession adds a test session to the server
func (s *Server) NewSession() *Session {
	session := &Session{
		ID:             s.Index,
		server:         s,
		sequences:      make(map[uint64]chan struct{}),
		mu:             sync.Mutex{},
		SequenceNumber: 0,
		streams:        make(map[uint64]*Stream),
	}
	s.sessions[session.ID] = session
	return session
}

// GetSession gets a test session
func (s *Server) GetSession(id uint64) (*Session, error) {
	sess, ok := s.sessions[id]
	if ok {
		return sess, nil
	}
	return sess, errors.New("session not found")
}

// Session manages a session, orders session operations, and manages streams for the session
type Session struct {
	ID             uint64
	server         *Server
	sequences      map[uint64]chan struct{}
	mu             sync.Mutex
	SequenceNumber uint64
	streams        map[uint64]*Stream
}

// NewStream creates a new stream for the session
func (s *Session) NewStream(c chan<- interface{}) *Stream {
	stream := &Stream{
		ID:         s.server.Index,
		session:    s,
		ItemNumber: 0,
		c:          c,
	}
	s.streams[stream.ID] = stream
	return stream
}

// Streams returns a slice of all streams open for the sesssion
func (s *Session) Streams() []*Stream {
	streams := make([]*Stream, 0)
	for _, value := range s.streams {
		streams = append(streams, value)
	}
	return streams
}

// getLatch returns a channel on which to wait for operations to be ordered for the given sequence number
func (s *Session) getLatch(sequence uint64) chan struct{} {
	s.mu.Lock()
	if _, ok := s.sequences[sequence]; !ok {
		s.sequences[sequence] = make(chan struct{}, 1)
	}
	latch := s.sequences[sequence]
	s.mu.Unlock()
	return latch
}

// Await waits for all commands prior to the given sequence number to be applied to the server
func (s *Session) Await(sequence uint64) {
	<-s.getLatch(sequence)
	s.SequenceNumber = sequence
}

// Complete unblocks the command following the given sequence number to be applied to the server
func (s *Session) Complete(sequence uint64) {
	s.getLatch(sequence + 1) <- struct{}{}
}

// NewResponseHeaders creates a new response header with headers for all open streams
func (s *Session) NewResponseHeader() (*headers.ResponseHeader, error) {
	return &headers.ResponseHeader{
		SessionID:  s.ID,
		Index:      s.server.Index,
		ResponseID: s.SequenceNumber,
	}, nil
}

// Delete deletes the test stream
func (s *Session) Delete() {
	delete(s.server.sessions, s.ID)
}

// Stream manages ordering for a single stream
type Stream struct {
	session    *Session
	ID         uint64
	ItemNumber uint64
	c          chan<- interface{}
}

// header creates a new stream header
func (s *Stream) Header(index uint64) *headers.StreamHeader {
	return &headers.StreamHeader{
		StreamID:   s.ID,
		ResponseID: s.ItemNumber,
	}
}

// NewResponseHeaders returns headers for the stream
func (s *Stream) NewResponseHeader() *headers.ResponseHeader {
	return &headers.ResponseHeader{
		SessionID:  s.ID,
		Index:      s.session.server.Index,
		ResponseID: s.session.SequenceNumber,
	}
}

// Send sends an EventResponse on the stream
func (s *Stream) Send(response interface{}) {
	s.ItemNumber++
	s.c <- response
}

// Delete deletes the test stream
func (s *Stream) Delete() {
	delete(s.session.streams, s.ID)
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
		<-c
		s.Stop()
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
		_ = c.Close()
	}()

	return c, stop
}

func StopTestServer(c chan struct{}) {
	c <- struct{}{}
}
