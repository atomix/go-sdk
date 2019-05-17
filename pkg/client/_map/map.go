package _map

import (
	"context"
	"errors"
	"github.com/atomix/atomix-go/pkg/client/protocol"
	"github.com/atomix/atomix-go/pkg/client/session"
	pb "github.com/atomix/atomix-go/proto/map"
	"google.golang.org/grpc"
	"io"
	"k8s.io/klog/glog"
)

func NewMap(conn *grpc.ClientConn, name string, protocol *protocol.Protocol, opts ...session.Option) (*Map, error) {
	c := pb.NewMapServiceClient(conn)
	s := newSession(c, name, protocol, opts...)
	if err := s.Connect(); err != nil {
		return nil, err
	}

	return &Map{
		client:  c,
		session: s,
	}, nil
}

type Map struct {
	client  pb.MapServiceClient
	session *Session
}

func (m *Map) Listen(ctx context.Context, c chan<- *MapEvent) error {
	request := &pb.EventRequest{
		Id:      m.session.mapId,
		Headers: m.session.Headers.GetCommandHeaders(),
	}
	events, err := m.client.Events(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		for {
			response, err := events.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				glog.Error("Failed to receive event stream", err)
			}

			var t MapEventType
			switch response.Type {
			case pb.EventResponse_INSERTED:
				t = EVENT_INSERTED
			case pb.EventResponse_UPDATED:
				t = EVENT_UPDATED
			case pb.EventResponse_REMOVED:
				t = EVENT_REMOVED
			}

			if m.session.Headers.Validate(response.Header) {
				c <- &MapEvent{
					Type:    t,
					Key:     response.Key,
					Value:   response.NewValue,
					Version: response.NewVersion,
				}
			}
		}
	}()
	return nil
}

func (m *Map) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*KeyValue, error) {
	partition := m.session.Headers.GetPartition(key)

	request := &pb.PutRequest{
		Id:     m.session.mapId,
		Header: partition.GetCommandHeader(),
		Key:    key,
		Value:  value,
	}

	for i := range opts {
		opts[i].before(request)
	}

	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].after(response)
	}

	partition.Update(response.Header)

	if response.Status == pb.ResponseStatus_OK {
		return &KeyValue{
			Key:     key,
			Value:   value,
			Version: int64(response.Header.Index),
		}, nil
	} else if response.Status == pb.ResponseStatus_PRECONDITION_FAILED {
		return nil, errors.New("write condition failed")
	} else if response.Status == pb.ResponseStatus_WRITE_LOCK {
		return nil, errors.New("write lock failed")
	} else {
		return &KeyValue{
			Key:     key,
			Value:   value,
			Version: int64(response.PreviousVersion),
		}, nil
	}
}

type PutOption interface {
	before(request *pb.PutRequest)
	after(response *pb.PutResponse)
}

func PutIfVersion(version int64) PutOption {
	return PutIfVersionOption{version: version}
}

type PutIfVersionOption struct {
	version int64
}

func (o PutIfVersionOption) before(request *pb.PutRequest) {
	request.Version = o.version
}

func (o PutIfVersionOption) after(response *pb.PutResponse) {

}

func (m *Map) Get(ctx context.Context, key string, opts ...GetOption) (*KeyValue, error) {
	partition := m.session.Headers.GetPartition(key)

	request := &pb.GetRequest{
		Id:     m.session.mapId,
		Header: partition.GetQueryHeader(),
		Key:    key,
	}

	for i := range opts {
		opts[i].before(request)
	}

	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].after(response)
	}

	partition.Update(response.Header)

	if response.Version != 0 {
		return &KeyValue{
			Key:     key,
			Value:   response.Value,
			Version: response.Version,
		}, nil
	}
	return nil, nil
}

type GetOption interface {
	before(request *pb.GetRequest)
	after(response *pb.GetResponse)
}

func GetOrDefault(def []byte) GetOption {
	return GetOrDefaultOption{def: def}
}

type GetOrDefaultOption struct {
	def []byte
}

func (o GetOrDefaultOption) before(request *pb.GetRequest) {
}

func (o GetOrDefaultOption) after(response *pb.GetResponse) {
	if response.Version == 0 {
		response.Value = o.def
	}
}

func (m *Map) Remove(ctx context.Context, key string, opts ...RemoveOption) (*KeyValue, error) {
	partition := m.session.Headers.GetPartition(key)

	request := &pb.RemoveRequest{
		Id:     m.session.mapId,
		Header: partition.GetCommandHeader(),
		Key:    key,
	}

	for i := range opts {
		opts[i].before(request)
	}

	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].after(response)
	}

	partition.Update(response.Header)

	if response.Status == pb.ResponseStatus_OK {
		return &KeyValue{
			Key:     key,
			Value:   response.PreviousValue,
			Version: response.PreviousVersion,
		}, nil
	} else if response.Status == pb.ResponseStatus_PRECONDITION_FAILED {
		return nil, errors.New("write condition failed")
	} else if response.Status == pb.ResponseStatus_WRITE_LOCK {
		return nil, errors.New("write lock failed")
	} else {
		return nil, nil
	}
}

type RemoveOption interface {
	before(request *pb.RemoveRequest)
	after(response *pb.RemoveResponse)
}

func RemoveIfVersion(version int64) RemoveOption {
	return RemoveIfVersionOption{version: version}
}

type RemoveIfVersionOption struct {
	version int64
}

func (o RemoveIfVersionOption) before(request *pb.RemoveRequest) {
	request.Version = o.version
}

func (o RemoveIfVersionOption) after(response *pb.RemoveResponse) {

}

func (m *Map) Size(ctx context.Context) (int, error) {
	request := &pb.SizeRequest{
		Id:      m.session.mapId,
		Headers: m.session.Headers.GetQueryHeaders(),
	}

	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	m.session.Headers.Update(response.Headers)
	return int(response.Size), nil
}

func (m *Map) Clear(ctx context.Context) error {
	request := &pb.ClearRequest{
		Headers: m.session.Headers.GetCommandHeaders(),
	}

	response, err := m.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	m.session.Headers.Update(response.Headers)
	return nil
}

func (m *Map) Close() error {
	return m.session.Close()
}

type KeyValue struct {
	Version int64
	Key     string
	Value   []byte
}

type MapEventType string

const (
	EVENT_INSERTED MapEventType = "inserted"
	EVENT_UPDATED  MapEventType = "updated"
	EVENT_REMOVED  MapEventType = "removed"
)

type MapEvent struct {
	Type    MapEventType
	Key     string
	Value   []byte
	Version int64
}
