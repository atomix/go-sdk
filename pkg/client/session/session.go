package session

import (
	"github.com/atomix/atomix-go-client/pkg/client/protocol"
	"github.com/atomix/atomix-go-client/proto/headers"
	"hash/fnv"
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
	raft          protocol.MultiRaftProtocol
	primaryBackup protocol.MultiPrimaryProtocol
	log           protocol.MultiLogProtocol
	timeout       time.Duration
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
	Timeout time.Duration
	Headers *Headers
	stopped chan struct{}
}

func (s *Session) Start(headers []*headers.SessionHeader) {
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
	session      *Session
	partitions   map[uint32]*Partition
	partitionIds []uint32
	mu           sync.RWMutex
}

func (h *Headers) Create(headers []*headers.SessionHeader) {
	h.mu.Lock()
	h.partitions = make(map[uint32]*Partition, len(headers))
	h.partitionIds = []uint32{}
	for _, header := range headers {
		id := header.PartitionId
		h.partitions[id] = &Partition{
			Id:        header.PartitionId,
			SessionId: header.SessionId,
			streams:   map[uint64]*Stream{},
		}
		h.partitionIds = append(h.partitionIds, header.PartitionId)
	}
	h.mu.Unlock()
}

func (h *Headers) Update(headers []*headers.SessionResponseHeader) {
	h.mu.Lock()
	for _, header := range headers {
		partition := h.partitions[header.PartitionId]
		partition.Update(header)
	}
	h.mu.Unlock()
}

func (h *Headers) Validate(header *headers.SessionResponseHeader) bool {
	h.mu.Lock()
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

func (h *Headers) GetSessionHeaders() []*headers.SessionHeader {
	h.mu.RLock()
	sh := []*headers.SessionHeader{}
	for _, partition := range h.partitions {
		sh = append(sh, partition.GetSessionHeader())
	}
	h.mu.RUnlock()
	return sh
}

func (h *Headers) GetQueryHeaders() []*headers.SessionQueryHeader {
	h.mu.RLock()
	qh := []*headers.SessionQueryHeader{}
	for _, partition := range h.partitions {
		qh = append(qh, partition.GetQueryHeader())
	}
	h.mu.RUnlock()
	return qh
}

func (h *Headers) GetCommandHeaders() []*headers.SessionCommandHeader {
	h.mu.RLock()
	ch := []*headers.SessionCommandHeader{}
	for _, partition := range h.partitions {
		ch = append(ch, partition.GetCommandHeader())
	}
	h.mu.RUnlock()
	return ch
}

func (h *Headers) GetPartition(key string) *Partition {
	if len(h.partitionIds) == 1 {
		return h.partitions[h.partitionIds[0]]
	}

	hash := fnv.New32()
	hash.Write([]byte(key))
	i := hash.Sum32()
	return h.partitions[h.partitionIds[i%uint32(len(h.partitionIds))]]
}

type Partition struct {
	Id                 uint32
	SessionId          uint64
	lastIndex          uint64
	sequenceNumber     uint64
	lastSequenceNumber uint64
	streams            map[uint64]*Stream
}

func (p *Partition) Update(header *headers.SessionResponseHeader) {
	if header.Index > p.lastIndex {
		p.lastIndex = header.Index
	}

	streams := p.streams
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

func (p *Partition) GetSessionHeader() *headers.SessionHeader {
	return &headers.SessionHeader{
		PartitionId:        p.Id,
		SessionId:          p.SessionId,
		LastSequenceNumber: p.lastSequenceNumber,
		Streams:            p.GetStreamHeaders(),
	}
}

func (p *Partition) GetCommandHeader() *headers.SessionCommandHeader {
	p.sequenceNumber += 1
	return &headers.SessionCommandHeader{
		PartitionId:    p.Id,
		SessionId:      p.SessionId,
		SequenceNumber: p.sequenceNumber,
	}
}

func (p *Partition) GetQueryHeader() *headers.SessionQueryHeader {
	return &headers.SessionQueryHeader{
		PartitionId:        p.Id,
		SessionId:          p.SessionId,
		LastIndex:          p.lastIndex,
		LastSequenceNumber: p.lastSequenceNumber,
	}
}

func (p *Partition) GetStreamHeaders() []*headers.SessionStreamHeader {
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
