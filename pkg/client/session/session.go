package session

import (
	"context"
	headers "github.com/atomix/atomix-go-client/proto/atomix/headers"
	"k8s.io/apimachinery/pkg/util/wait"
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
	Create(session *Session) error
	KeepAlive(session *Session) error
	Close(session *Session) error
}

func New(ctx context.Context, namespace string, name string, handler Handler, opts ...SessionOption) (*Session, error) {
	options := &sessionOptions{}
	for i := range opts {
		opts[i].prepare(options)
	}
	session := &Session{
		Name: &headers.Name{
			Namespace: namespace,
			Name:      name,
		},
		handler: handler,
		Timeout: options.timeout,
		streams: make(map[uint64]*Stream),
		mu:      sync.RWMutex{},
		stopped: make(chan struct{}),
	}
	if err := session.start(); err != nil {
		return nil, err
	}
	return session, nil
}

type Session struct {
	Name               *headers.Name
	Timeout            time.Duration
	SessionId          uint64
	handler            Handler
	lastIndex          uint64
	sequenceNumber     uint64
	lastSequenceNumber uint64
	streams            map[uint64]*Stream
	mu                 sync.RWMutex
	stopped            chan struct{}
}

func (s *Session) start() error {
	err := s.handler.Create(s)
	if err != nil {
		return err
	}

	go wait.Until(func() {
		s.handler.KeepAlive(s)
	}, s.Timeout/2, s.stopped)
	return nil
}

func (s *Session) Close() error {
	err := s.handler.Close(s)
	close(s.stopped)
	return err
}

func (s *Session) GetHeader() *headers.RequestHeader {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &headers.RequestHeader{
		Name:           s.Name,
		SessionId:      s.SessionId,
		Index:          s.lastIndex,
		SequenceNumber: s.sequenceNumber,
		Streams:        s.getStreamHeaders(),
	}
}

func (s *Session) NextHeader() *headers.RequestHeader {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequenceNumber = s.sequenceNumber + 1
	return &headers.RequestHeader{
		Name:           s.Name,
		SessionId:      s.SessionId,
		Index:          s.lastIndex,
		SequenceNumber: s.sequenceNumber,
		Streams:        s.getStreamHeaders(),
	}
}

func (s *Session) UpdateHeader(header *headers.ResponseHeader) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if header.SessionId > s.SessionId {
		s.SessionId = header.SessionId
	}
	if header.Index > s.lastIndex {
		s.lastIndex = header.Index
	}

	streams := s.streams
	for j := range header.Streams {
		id := header.Streams[j].StreamId
		if _, ok := streams[id]; !ok {
			streams[id] = &Stream{
				id:    id,
				index: id,
			}
		}
	}
}

func (s *Session) ValidStream(header *headers.ResponseHeader) bool {
	s.mu.Lock()
	if header.Index > s.lastIndex {
		s.lastIndex = header.Index
	}

	streamHeader := header.Streams[0]
	stream, ok := s.streams[streamHeader.StreamId]
	if !ok {
		stream = &Stream{
			id:    streamHeader.StreamId,
			index: streamHeader.StreamId,
		}
		s.streams[streamHeader.StreamId] = stream
	}

	defer s.mu.Unlock()

	if streamHeader.Index >= s.lastIndex && streamHeader.LastItemNumber == stream.lastItemNumber+1 {
		s.lastIndex = streamHeader.Index
		stream.lastItemNumber = streamHeader.LastItemNumber
		return true
	}
	return false
}

func (s *Session) getStreamHeaders() []*headers.StreamHeader {
	result := make([]*headers.StreamHeader, len(s.streams))
	for _, stream := range s.streams {
		result = append(result, stream.newStreamHeader())
	}
	return result
}

type Stream struct {
	id             uint64
	index          uint64
	lastItemNumber uint64
}

func (s *Stream) newStreamHeader() *headers.StreamHeader {
	return &headers.StreamHeader{
		StreamId:       s.id,
		Index:          s.index,
		LastItemNumber: s.lastItemNumber,
	}
}
