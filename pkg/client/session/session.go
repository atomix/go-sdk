package session

import (
	"github.com/atomix/atomix-go/pkg/client"
	"github.com/atomix/atomix-go/proto/headers"
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
	raft client.MultiRaftProtocol
	primaryBackup client.MultiPrimaryProtocol
	log client.MultiLogProtocol
	timeout time.Duration
}

func NewSession(opts ...Option) *Session {
	headers := NewHeaders()
	options := &Options{}
	for i := range opts {
		opts[i].prepare(options)
	}
	session := &Session{
		Timeout: options.timeout,
	}
	headers.session = session
	session.Headers = headers
	return session
}

type Session struct {
	Id      uint64
	Timeout time.Duration
	Headers *Headers
	stopped chan struct{}
}

func (s *Session) Start(headers *headers.SessionHeaders) {
	s.Id = headers.SessionId
	s.Headers.Create(headers)

	go wait.Until(func() {
		s.keepAlive()
	}, s.Timeout/2, s.stopped)
}

func (s *Session) keepAlive() error {
	return nil
}

func (s *Session) Stop() {
	close(s.stopped)
}

func NewHeaders() *Headers {
	return &Headers{
		mu: sync.RWMutex{},
	}
}

type Headers struct {
	session    *Session
	partitions map[uint32]*Partition
	mu         sync.RWMutex
}

func (h *Headers) Create(headers *headers.SessionHeaders) {
	h.mu.Lock()
	h.partitions = make(map[uint32]*Partition, len(headers.Headers))
	for i := range headers.Headers {
		id := headers.Headers[i].PartitionId
		h.partitions[id] = &Partition{
			id:      headers.Headers[i].PartitionId,
			streams: map[uint64]*Stream{},
		}
	}
	h.mu.Unlock()
}

func (h *Headers) Update(headers *headers.SessionResponseHeaders) {
	h.mu.Lock()
	for i := range headers.Headers {
		header := headers.Headers[i]
		partition := h.partitions[header.PartitionId]
		if header.Index > partition.lastIndex {
			partition.lastIndex = header.Index
		}

		streams := partition.streams
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
	h.mu.Unlock()
}

func (h *Headers) Validate(headers *headers.SessionResponseHeaders) bool {
	h.mu.Lock()
	header := headers.Headers[0]
	partition := h.partitions[header.PartitionId]
	if header.Index > partition.lastIndex {
		partition.lastIndex = header.Index
	}

	streamHeader := header.Streams[0]
	stream, ok := partition.streams[streamHeader.StreamId]
	if !ok {
		stream = &Stream{
			id:    streamHeader.StreamId,
			index: streamHeader.StreamId,
		}
		partition.streams[streamHeader.StreamId] = stream
	}

	defer h.mu.Unlock()

	if streamHeader.Index >= partition.lastIndex && streamHeader.LastItemNumber == stream.lastItemNumber+1 {
		partition.lastIndex = streamHeader.Index
		stream.lastItemNumber = streamHeader.LastItemNumber
		return true
	}
	return false
}

func (h *Headers) Session() *headers.SessionHeaders {
	h.mu.RLock()
	sh := make([]*headers.SessionHeader, len(h.partitions))
	for i := range h.partitions {
		sh[i] = h.partitions[i].newSessionHeader()
	}
	h.mu.RUnlock()

	return &headers.SessionHeaders{
		SessionId: h.session.Id,
		Headers:   sh,
	}
}

func (h *Headers) Query() *headers.SessionQueryHeaders {
	h.mu.RLock()
	qh := make([]*headers.SessionQueryHeader, len(h.partitions))
	for i := range h.partitions {
		qh[i] = h.partitions[i].newQueryHeader()
	}
	h.mu.RUnlock()

	return &headers.SessionQueryHeaders{
		SessionId: h.session.Id,
		Headers:   qh,
	}
}

func (h *Headers) Command() *headers.SessionCommandHeaders {
	h.mu.RLock()
	ch := make([]*headers.SessionCommandHeader, len(h.partitions))
	for i := range h.partitions {
		ch[i] = h.partitions[i].newCommandHeader()
	}
	h.mu.RUnlock()

	return &headers.SessionCommandHeaders{
		SessionId: h.session.Id,
		Headers:   ch,
	}
}

type Partition struct {
	id                 uint32
	lastIndex          uint64
	sequenceNumber     uint64
	lastSequenceNumber uint64
	streams            map[uint64]*Stream
}

func (p *Partition) newSessionHeader() *headers.SessionHeader {
	return &headers.SessionHeader{
		PartitionId:        p.id,
		LastSequenceNumber: p.lastSequenceNumber,
		Streams:            p.newStreamHeaders(),
	}
}

func (p *Partition) newCommandHeader() *headers.SessionCommandHeader {
	p.sequenceNumber += 1
	return &headers.SessionCommandHeader{
		PartitionId:    p.id,
		SequenceNumber: p.sequenceNumber,
	}
}

func (p *Partition) newQueryHeader() *headers.SessionQueryHeader {
	return &headers.SessionQueryHeader{
		PartitionId:        p.id,
		LastIndex:          p.lastIndex,
		LastSequenceNumber: p.lastSequenceNumber,
	}
}

func (p *Partition) newStreamHeaders() []*headers.SessionStreamHeader {
	result := make([]*headers.SessionStreamHeader, len(p.streams))
	for _, stream := range p.streams {
		result = append(result, stream.newStreamHeader())
	}
	return result
}

type Stream struct {
	id             uint64
	index          uint64
	lastItemNumber uint64
}

func (s *Stream) newStreamHeader() *headers.SessionStreamHeader {
	return &headers.SessionStreamHeader{
		StreamId:       s.id,
		Index:          s.index,
		LastItemNumber: s.lastItemNumber,
	}
}
