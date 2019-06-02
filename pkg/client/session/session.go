package session

import (
	"github.com/atomix/atomix-go-client/proto/atomix/headers"
	"k8s.io/apimachinery/pkg/util/wait"
	"sync"
	"time"
)

type Interface interface {
	Connect() error
	keepAlive() error
	Close() error
}

type Option interface {
	prepare(options *Options)
}

func Timeout(timeout time.Duration) Option {
	return TimeoutOption{timeout: timeout}
}

type TimeoutOption struct {
	timeout time.Duration
}

func (o TimeoutOption) prepare(options *Options) {
	options.timeout = o.timeout
}

type Options struct {
	timeout time.Duration
}

type SessionClient interface {
	Create(header atomix_headers.RequestHeader) (*atomix_headers.ResponseHeader, error)
	KeepAlive(header atomix_headers.RequestHeader) (*atomix_headers.ResponseHeader, error)
	Close(header atomix_headers.RequestHeader) (*atomix_headers.ResponseHeader, error)
}

func NewSession(namespace string, name string, opts ...Option) *Session {
	options := &Options{}
	for i := range opts {
		opts[i].prepare(options)
	}
	session := &Session{
		Name: &atomix_headers.Name{
			Namespace: namespace,
			Name:      name,
		},
		Timeout: options.timeout,
		streams: make(map[uint64]*Stream),
		mu:      sync.RWMutex{},
		stopped: make(chan struct{}),
	}
	return session
}

type Session struct {
	Name               *atomix_headers.Name
	Timeout            time.Duration
	SessionId          uint64
	lastIndex          uint64
	sequenceNumber     uint64
	lastSequenceNumber uint64
	streams            map[uint64]*Stream
	mu                 sync.RWMutex
	stopped            chan struct{}
}

func (s *Session) Start() error {
	header, err := s.connect(&atomix_headers.RequestHeader{
		Name: s.Name,
	}, s.Timeout)
	if err != nil {
		return err
	}

	if header != nil {
		s.UpdateHeader(header)
		go wait.Until(func() {
			s.keepAlive(s.GetHeader())
		}, s.Timeout/2, s.stopped)
	}
	return nil
}

func (s *Session) connect(header *atomix_headers.RequestHeader, timeout time.Duration) (*atomix_headers.ResponseHeader, error) {
	return nil, nil
}

func (s *Session) keepAlive(header *atomix_headers.RequestHeader) (*atomix_headers.ResponseHeader, error) {
	return nil, nil
}

func (s *Session) Stop() error {
	err := s.close(s.GetHeader())
	close(s.stopped)
	return err
}

func (s *Session) close(header *atomix_headers.RequestHeader) error {
	return nil
}

func (s *Session) GetHeader() *atomix_headers.RequestHeader {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &atomix_headers.RequestHeader{
		Name:           s.Name,
		SessionId:      s.SessionId,
		Index:          s.lastIndex,
		SequenceNumber: s.sequenceNumber,
		Streams:        s.getStreamHeaders(),
	}
}

func (s *Session) NextHeader() *atomix_headers.RequestHeader {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequenceNumber = s.sequenceNumber + 1
	return &atomix_headers.RequestHeader{
		Name:           s.Name,
		SessionId:      s.SessionId,
		Index:          s.lastIndex,
		SequenceNumber: s.sequenceNumber,
		Streams:        s.getStreamHeaders(),
	}
}

func (s *Session) UpdateHeader(header *atomix_headers.ResponseHeader) {
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

func (s *Session) ValidStream(header *atomix_headers.ResponseHeader) bool {
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

func (s *Session) getStreamHeaders() []*atomix_headers.StreamHeader {
	result := make([]*atomix_headers.StreamHeader, len(s.streams))
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

func (s *Stream) newStreamHeader() *atomix_headers.StreamHeader {
	return &atomix_headers.StreamHeader{
		StreamId:       s.id,
		Index:          s.index,
		LastItemNumber: s.lastItemNumber,
	}
}
