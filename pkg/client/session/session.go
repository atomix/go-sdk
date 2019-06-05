package session

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
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
		Name: &headers.Name{
			Namespace: name.Application,
			Name:      name.Name,
		},
		handler: handler,
		Timeout: options.timeout,
		streams: make(map[uint64]*Stream),
		mu:      sync.RWMutex{},
		stopped: make(chan struct{}),
	}
	if err := session.start(ctx); err != nil {
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

func (s *Session) start(ctx context.Context) error {
	err := s.handler.Create(ctx, s)
	if err != nil {
		return err
	}

	go wait.Until(func() {
		s.handler.KeepAlive(context.TODO(), s)
	}, s.Timeout/2, s.stopped)
	return nil
}

func (s *Session) Close() error {
	err := s.handler.Close(context.TODO(), s)
	close(s.stopped)
	return err
}

func (s *Session) Delete() error {
	err := s.handler.Delete(context.TODO(), s)
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
		streamHeader := header.Streams[j]
		id := streamHeader.StreamId
		stream, ok := streams[id]
		if ok {
			stream.complete(streamHeader)
		} else {
			streams[id] = newStream(id)
		}
	}
}

func (s *Session) WaitStream(header *headers.StreamHeader) chan struct{} {
	ch := make(chan struct{}, 1)

	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[header.StreamId]

	// If the stream does not exist, close and return the channel.
	if !ok {
		close(ch)
		return ch
	}

	// If the response was not received in stream order, skip the response.
	if header.LastItemNumber != stream.lastItemNumber+1 {
		close(ch)
		return ch
	}

	// Update the stream received index and last item number to ensure the next response can be handled.
	stream.receivedIndex = header.Index
	stream.lastItemNumber = header.LastItemNumber

	// If the response index for the session has advanced to the stream index, complete the stream.
	if s.lastIndex >= header.Index {
		stream.completeIndex = header.Index
		stream.lastItemNumber = header.LastItemNumber
		ch <- struct{}{}
		close(ch)
		return ch
	}

	// If the response index has not advanced to the stream index, enqueue the channel to be completed
	// once the session has advanced to the response index.
	queue, ok := stream.queue[header.Index]
	if !ok {
		queue := []chan struct{}{
			ch,
		}
		stream.queue[header.Index] = queue
	} else {
		stream.queue[header.Index] = append(queue, ch)
	}
	return ch
}

func (s *Session) getStreamHeaders() []*headers.StreamHeader {
	result := make([]*headers.StreamHeader, 0, len(s.streams))
	for _, stream := range s.streams {
		result = append(result, stream.newStreamHeader())
	}
	return result
}

func newStream(id uint64) *Stream {
	return &Stream{
		id:            id,
		receivedIndex: id,
		completeIndex: id,
		queue: make(map[uint64][]chan struct{}),
	}
}

type Stream struct {
	id             uint64
	receivedIndex  uint64
	completeIndex  uint64
	lastItemNumber uint64
	queue          map[uint64][]chan struct{}
}

func (s *Stream) newStreamHeader() *headers.StreamHeader {
	return &headers.StreamHeader{
		StreamId:       s.id,
		Index:          s.receivedIndex,
		LastItemNumber: s.lastItemNumber,
	}
}

func (s *Stream) complete(header *headers.StreamHeader) {
	for s.completeIndex < header.Index {
		s.completeIndex += 1
		queue, ok := s.queue[s.completeIndex]
		if ok {
			for _, ch := range queue {
				ch <- struct{}{}
			}
			delete(s.queue, s.completeIndex)
		}
	}
}
