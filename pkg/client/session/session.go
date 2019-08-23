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

package session

import (
	"context"
	"github.com/atomix/atomix-api/proto/atomix/headers"
	api "github.com/atomix/atomix-api/proto/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"sync"
	"time"
)

// Option implements a session option
type Option interface {
	prepare(options *options)
}

// WithTimeout returns a session Option to configure the session timeout
func WithTimeout(timeout time.Duration) Option {
	return timeoutOption{timeout: timeout}
}

type timeoutOption struct {
	timeout time.Duration
}

func (o timeoutOption) prepare(options *options) {
	options.timeout = o.timeout
}

type options struct {
	timeout time.Duration
}

// Handler provides session management for a primitive implementation
type Handler interface {
	// Create is called to create the session
	Create(ctx context.Context, session *Session) error

	// KeepAlive is called periodically to keep the session alive
	KeepAlive(ctx context.Context, session *Session) error

	// Close is called to close the session
	Close(ctx context.Context, session *Session) error

	// Delete is called to delete the primitive
	Delete(ctx context.Context, session *Session) error
}

// New creates a new Session for the given primitive
// name is the name of the primitive
// handler is the primitive's session handler
func New(ctx context.Context, name primitive.Name, handler Handler, opts ...Option) (*Session, error) {
	options := &options{}
	WithTimeout(30 * time.Second).prepare(options)
	for i := range opts {
		opts[i].prepare(options)
	}
	session := &Session{
		Name: &api.Name{
			Namespace: name.Application,
			Name:      name.Name,
		},
		handler: handler,
		Timeout: options.timeout,
		streams: make(map[uint64]*Stream),
		mu:      sync.RWMutex{},
		ticker:  time.NewTicker(options.timeout / 2),
	}
	if err := session.start(ctx); err != nil {
		return nil, err
	}
	return session, nil
}

// Session maintains the session for a primitive
type Session struct {
	Name       *api.Name
	Timeout    time.Duration
	SessionID  uint64
	handler    Handler
	lastIndex  uint64
	requestID  uint64
	responseID uint64
	streams    map[uint64]*Stream
	mu         sync.RWMutex
	ticker     *time.Ticker
}

// start creates the session and begins keep-alives
func (s *Session) start(ctx context.Context) error {
	err := s.handler.Create(ctx, s)
	if err != nil {
		return err
	}

	go func() {
		for range s.ticker.C {
			_ = s.handler.KeepAlive(context.TODO(), s)
		}
	}()
	return nil
}

// Close closes the session
func (s *Session) Close() error {
	err := s.handler.Close(context.TODO(), s)
	s.ticker.Stop()
	return err
}

// Delete closes the session and deletes the primitive
func (s *Session) Delete() error {
	err := s.handler.Delete(context.TODO(), s)
	s.ticker.Stop()
	return err
}

// GetState gets the header for the current state of the session
func (s *Session) GetState() *headers.RequestHeader {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &headers.RequestHeader{
		Name:      s.Name,
		SessionID: s.SessionID,
		Index:     s.lastIndex,
		RequestID: s.responseID,
		Streams:   s.getStreamHeaders(),
	}
}

// GetRequest gets the current read header
func (s *Session) GetRequest() *headers.RequestHeader {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &headers.RequestHeader{
		Name:      s.Name,
		SessionID: s.SessionID,
		Index:     s.lastIndex,
		RequestID: s.requestID,
	}
}

// NextRequest returns the next write header
func (s *Session) NextRequest() *headers.RequestHeader {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestID = s.requestID + 1
	return &headers.RequestHeader{
		Name:      s.Name,
		SessionID: s.SessionID,
		Index:     s.lastIndex,
		RequestID: s.requestID,
	}
}

// RecordResponse records the index in a response header
func (s *Session) RecordResponse(requestHeader *headers.RequestHeader, responseHeader *headers.ResponseHeader) {
	// Use a double-checked lock to avoid locking when multiple responses are received for an index.
	if responseHeader.Index > s.lastIndex {
		s.mu.Lock()
		defer s.mu.Unlock()

		// If the session ID is set, ensure the session is initialized
		if responseHeader.SessionID > s.SessionID {
			s.SessionID = responseHeader.SessionID
			s.lastIndex = responseHeader.SessionID
		}

		// If the request ID is greater than the highest response ID, update the response ID.
		if requestHeader.RequestID > s.responseID {
			s.responseID = requestHeader.RequestID
		}

		// If the response index has increased, update the last received index
		if responseHeader.Index > s.lastIndex {
			s.lastIndex = responseHeader.Index
		}
	}
}

// NewStream creates a new stream
func (s *Session) NewStream(streamID uint64) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := &Stream{
		ID:      streamID,
		session: s,
	}
	s.streams[streamID] = stream
	return stream
}

// deleteStream deletes the given stream from the session
func (s *Session) deleteStream(streamID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.streams, streamID)
}

// getStreamHeaders returns a slice of headers for all open streams
func (s *Session) getStreamHeaders() []*headers.StreamHeader {
	result := make([]*headers.StreamHeader, 0, len(s.streams))
	for _, stream := range s.streams {
		result = append(result, stream.getHeader())
	}
	return result
}

// Stream manages the context for a single response stream within a session
type Stream struct {
	ID         uint64
	session    *Session
	responseID uint64
	mu         sync.RWMutex
}

// getHeader returns the current header for the stream
func (s *Stream) getHeader() *headers.StreamHeader {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &headers.StreamHeader{
		StreamID:   s.ID,
		ResponseID: s.responseID,
	}
}

// Serialize updates the stream response metadata and returns whether the response was received in sequential order
func (s *Stream) Serialize(header *headers.ResponseHeader) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if header.ResponseID == s.responseID+1 {
		s.responseID++
		return true
	}
	return false
}

// Close closes the stream
func (s *Stream) Close() {
	s.session.deleteStream(s.ID)
}
