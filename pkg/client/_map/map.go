package _map

import (
	"context"
	"fmt"
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
		client: c,
		session: s,
	}, nil
}

type Map struct {
	client  pb.MapServiceClient
	session *Session
}

func (m *Map) Listen(ctx context.Context, c chan<- *MapEvent) error {
	request := &pb.EventRequest{
		Id: m.session.mapId,
		Headers: m.session.Headers.Command(),
	}
	events, err := m.client.Events(ctx, request)
	if err != nil {
		return err
	}

	ec := make(chan *MapEvent)
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

			if m.session.Headers.Validate(response.Headers) {
				ec<-&MapEvent{
					Type: t,
					Key: response.Key,
					Value: response.NewValue,
					Version: response.NewVersion,
				}
			}
		}
	}()

	for {
		select {
		case e := <-ec:
			c<-e
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *Map) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*KeyValue, error) {
	request := &pb.PutRequest{
		Id: m.session.mapId,
		Headers: m.session.Headers.Command(),
		Key: key,
		Value: value,
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

	m.session.Headers.Update(response.Headers)

	return &KeyValue{
		Key: key,
		Value: response.PreviousValue,
		Version: response.PreviousVersion,
	}, nil
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
	request := &pb.GetRequest{
		Id: m.session.mapId,
		Headers: m.session.Headers.Query(),
		Key: key,
	}
	fmt.Printf("%v", request)

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

	m.session.Headers.Update(response.Headers)

	return &KeyValue{
		Key: key,
		Value: response.Value,
		Version: response.Version,
	}, nil
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
	request := &pb.RemoveRequest{
		Id: m.session.mapId,
		Headers: m.session.Headers.Command(),
		Key: key,
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

	m.session.Headers.Update(response.Headers)

	return &KeyValue{
		Key: key,
	}, nil
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

func (m *Map) Size(ctx context.Context) (*int32, error) {
	request := &pb.SizeRequest{
		Id: m.session.mapId,
		Headers: m.session.Headers.Query(),
	}

	response, err := m.client.Size(ctx, request)
	if err != nil {
		return nil, err
	}

	m.session.Headers.Update(response.Headers)
	return &response.Size, nil
}

func (m *Map) Clear(ctx context.Context) error {
	request := &pb.ClearRequest{
		Headers: m.session.Headers.Command(),
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
	Key string
	Value []byte
}

type MapEventType string

const (
	EVENT_INSERTED MapEventType = "inserted"
	EVENT_UPDATED MapEventType = "updated"
	EVENT_REMOVED MapEventType = "removed"
)

type MapEvent struct {
	Type MapEventType
	Key string
	Value []byte
	Version int64
}
