package session

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/proto/atomix/headers"
	pbprimitive "github.com/atomix/atomix-go-client/proto/atomix/primitive"
	"sync"
	"time"
)

type SessionOption interface {
	prepare(options *sessionOptions)
}

func WithTimeout(timeout time.Duration) SessionOption {
	return timeoutOption{timeout: timeout}
}

type timeoutOption struct {
	timeout time.Duration
}

func (o timeoutOption) prepare(options *sessionOptions) {
	options.timeout = o.timeout
}

type sessionOptions struct {
	timeout time.Duration
}

type Handler interface {
	Create(ctx context.Context, session *Session) error
	KeepAlive(ctx context.Context, session *Session) error
	Close(ctx context.Context, session *Session) error
	Delete(ctx context.Context, session *Session) error
}

func New(ctx context.Context, name primitive.Name, handler Handler, opts ...SessionOption) (*Session, error) {
	options := &sessionOptions{}
	for i := range opts {
		opts[i].prepare(options)
	}
	session := &Session{
		Name: &pbprimitive.Name{
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

type Session struct {
	Name               *pbprimitive.Name
	Timeout            time.Duration
	SessionID          uint64
	handler            Handler
	lastIndex          uint64
	requestID          uint64
	lastSequenceNumber uint64
	streams            map[uint64]*Stream
	mu                 sync.RWMutex
	ticker             *time.Ticker
}

// start creates the session and begins keep-alives
func (s *Session) start(ctx context.Context) error {
	err := s.handler.Create(ctx, s)
	if err != nil {
		return err
	}

	go func() {
		for range s.ticker.C {
			s.handler.KeepAlive(context.TODO(), s)
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

// GetRequest gets the current read header
func (s *Session) GetRequest() *headers.RequestHeader {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &headers.RequestHeader{
		Name:      s.Name,
		SessionId: s.SessionID,
		Index:     s.lastIndex,
		RequestId: s.requestID,
	}
}

// NextRequest returns the next write header
func (s *Session) NextRequest() *headers.RequestHeader {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestID = s.requestID + 1
	return &headers.RequestHeader{
		Name:      s.Name,
		SessionId: s.SessionID,
		Index:     s.lastIndex,
		RequestId: s.requestID,
		Streams:   s.getStreamHeaders(),
	}
}

// RecordResponse records the index in a response header
func (s *Session) RecordResponse(header *headers.ResponseHeader) {
	// Use a double-checked lock to avoid locking when multiple responses are received for an index.
	if header.Index > s.lastIndex {
		s.mu.Lock()
		defer s.mu.Unlock()

		// If the session ID is set, ensure the session is initialized
		if header.SessionId > s.SessionID {
			s.SessionID = header.SessionId
			s.lastIndex = header.SessionId
		}

		// If the response index has increased, update the last received index
		if header.Index > s.lastIndex {
			s.lastIndex = header.Index
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
		StreamId:   s.ID,
		ResponseId: s.responseID,
	}
}

// Serialize updates the stream response metadata and returns whether the response was received in sequential order
func (s *Stream) Serialize(header *headers.ResponseHeader) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if header.ResponseId == s.responseID+1 {
		s.responseID++
		return true
	}
	return false
}

// Close closes the stream
func (s *Stream) Close() {
	s.session.deleteStream(s.ID)
}
