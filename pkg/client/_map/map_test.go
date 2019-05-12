package _map

import (
	"context"
	"errors"
	"github.com/atomix/atomix-go/pkg/client/protocol"
	"github.com/atomix/atomix-go/proto/headers"
	pb "github.com/atomix/atomix-go/proto/map"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
	"testing"
)

func NewTestServer() *TestServer {
	return &TestServer{
		sessions: make(map[uint64]uint64),
		index:    0,
		entries:  make(map[string]*KeyValue),
		streams:  make(map[uint64]*TestStream),
	}
}

type TestServer struct {
	sessions map[uint64]uint64
	index    uint64
	entries  map[string]*KeyValue
	streams  map[uint64]*TestStream
}

type TestStream struct {
	id uint64
	stream pb.MapService_EventsServer
	sequence uint64
}

func (s *TestStream) header(index uint64) *headers.SessionStreamHeader {
	return &headers.SessionStreamHeader{
		StreamId: s.id,
		Index: index,
		LastItemNumber: s.sequence,
	}
}

func (s *TestStream) Send(index uint64, response *pb.EventResponse) error {
	s.sequence += 1
	response.Headers.Headers[0].Streams = []*headers.SessionStreamHeader{
		{
			StreamId: s.id,
			Index: index,
			LastItemNumber: s.sequence,
		},
	}
	return s.stream.Send(response)
}

func (s *TestServer) incrementIndex() {
	s.index += 1
}

func (s *TestServer) Create(ctx context.Context, request *pb.CreateRequest) (*pb.CreateResponse, error) {
	s.incrementIndex()
	s.sessions[s.index] = 0
	return &pb.CreateResponse{
		Headers: &headers.SessionHeaders{
			SessionId: s.index,
			Headers: []*headers.SessionHeader{
				{
					PartitionId: 1,
				},
			},
		},
	}, nil
}

func (s TestServer) KeepAlive(ctx context.Context, request *pb.KeepAliveRequest) (*pb.KeepAliveResponse, error) {
	s.incrementIndex()
	if session, exists := s.sessions[request.Headers.SessionId]; exists {
		streams := []*headers.SessionStreamHeader{}
		for _, stream := range s.streams {
			streams = append(streams, stream.header(uint64(s.index)))
		}
		return &pb.KeepAliveResponse{
			Headers: &headers.SessionHeaders{
				SessionId: request.Headers.SessionId,
				Headers: []*headers.SessionHeader{
					{
						PartitionId:        1,
						LastSequenceNumber: session,
						Streams: streams,
					},
				},
			},
		}, nil
	} else {
		return nil, errors.New("session does not exist")
	}
}

func (s TestServer) Close(ctx context.Context, request *pb.CloseRequest) (*pb.CloseResponse, error) {
	s.incrementIndex()
	if _, exists := s.sessions[request.Headers.SessionId]; exists {
		return &pb.CloseResponse{}, nil
	} else {
		return nil, errors.New("session does not exist")
	}
}

func (s TestServer) Put(ctx context.Context, request *pb.PutRequest) (*pb.PutResponse, error) {
	if _, exists := s.sessions[request.Headers.SessionId]; exists {
		sequence := request.Headers.Headers[0].SequenceNumber
		s.sessions[request.Headers.SessionId] = sequence

		v := s.entries[request.Key]

		streams := []*headers.SessionStreamHeader{}
		for _, stream := range s.streams {
			streams = append(streams, stream.header(uint64(s.index)))
		}
		h := &headers.SessionResponseHeaders{
			SessionId: request.Headers.SessionId,
			Headers: []*headers.SessionResponseHeader{
				{
					PartitionId:    1,
					Index:          s.index,
					SequenceNumber: sequence,
					Streams: streams,
				},
			},
		}

		if request.Version != 0 && (v == nil || v.Version != request.Version) {
			return &pb.PutResponse{
				Headers: h,
				Status:  pb.ResponseStatus_PRECONDITION_FAILED,
			}, nil
		}

		s.entries[request.Key] = &KeyValue{
			Key:     request.Key,
			Value:   request.Value,
			Version: int64(s.index),
		}

		for _, stream := range s.streams {
			if v.Version == 0 {
				stream.Send(s.index, &pb.EventResponse{
					Headers:    h,
					Type:       pb.EventResponse_INSERTED,
					Key:        request.Key,
					NewValue:   request.Value,
					NewVersion: request.Version,
				})
			} else {
				stream.Send(s.index, &pb.EventResponse{
					Headers:    h,
					Type:       pb.EventResponse_UPDATED,
					Key:        request.Key,
					OldValue:   v.Value,
					OldVersion: v.Version,
					NewValue:   request.Value,
					NewVersion: request.Version,
				})
			}
		}

		if v != nil {
			return &pb.PutResponse{
				Headers:         h,
				Status:          pb.ResponseStatus_OK,
				PreviousValue:   v.Value,
				PreviousVersion: v.Version,
			}, nil
		} else {
			return &pb.PutResponse{
				Headers: h,
				Status:  pb.ResponseStatus_OK,
			}, nil
		}
	} else {
		return nil, errors.New("session does not exist")
	}
}

func (s TestServer) Get(ctx context.Context, request *pb.GetRequest) (*pb.GetResponse, error) {
	if sequence, exists := s.sessions[request.Headers.SessionId]; exists {
		v := s.entries[request.Key]

		streams := []*headers.SessionStreamHeader{}
		for _, stream := range s.streams {
			streams = append(streams, stream.header(uint64(s.index)))
		}
		h := &headers.SessionResponseHeaders{
			SessionId: request.Headers.SessionId,
			Headers: []*headers.SessionResponseHeader{
				{
					PartitionId:    1,
					Index:          s.index,
					SequenceNumber: sequence,
					Streams: streams,
				},
			},
		}

		if v.Version != 0 {
			return &pb.GetResponse{
				Headers: h,
				Value:   v.Value,
				Version: v.Version,
			}, nil
		} else {
			return &pb.GetResponse{
				Headers: h,
			}, nil
		}
	} else {
		return nil, errors.New("session does not exist")
	}
}

func (s TestServer) Remove(ctx context.Context, request *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	s.incrementIndex()
	if _, exists := s.sessions[request.Headers.SessionId]; exists {
		sequence := request.Headers.Headers[0].SequenceNumber
		s.sessions[request.Headers.SessionId] = sequence

		v := s.entries[request.Key]

		streams := []*headers.SessionStreamHeader{}
		for _, stream := range s.streams {
			streams = append(streams, stream.header(uint64(s.index)))
		}
		h := &headers.SessionResponseHeaders{
			SessionId: request.Headers.SessionId,
			Headers: []*headers.SessionResponseHeader{
				{
					PartitionId:    1,
					Index:          s.index,
					SequenceNumber: sequence,
					Streams: streams,
				},
			},
		}

		if v == nil || (request.Version != 0 && v.Version != request.Version) {
			return &pb.RemoveResponse{
				Headers: h,
				Status:  pb.ResponseStatus_PRECONDITION_FAILED,
			}, nil
		}

		delete(s.entries, request.Key)

		for _, stream := range s.streams {
			stream.Send(s.index, &pb.EventResponse{
				Headers:    h,
				Type:       pb.EventResponse_REMOVED,
				Key:        request.Key,
				OldValue:   v.Value,
				OldVersion: v.Version,
			})
		}

		if v.Version != 0 {
			return &pb.RemoveResponse{
				Headers:         h,
				Status:          pb.ResponseStatus_OK,
				PreviousValue:   v.Value,
				PreviousVersion: v.Version,
			}, nil
		} else {
			return &pb.RemoveResponse{
				Headers: h,
				Status:  pb.ResponseStatus_OK,
			}, nil
		}
	} else {
		return nil, errors.New("session does not exist")
	}
}

func (s TestServer) Replace(ctx context.Context, request *pb.ReplaceRequest) (*pb.ReplaceResponse, error) {
	return nil, errors.New("not implemented")
}

func (s TestServer) Exists(ctx context.Context, request *pb.ExistsRequest) (*pb.ExistsResponse, error) {
	return nil, errors.New("not implemented")
}

func (s TestServer) Size(ctx context.Context, request *pb.SizeRequest) (*pb.SizeResponse, error) {
	return nil, errors.New("not implemented")
}

func (s TestServer) Clear(ctx context.Context, request *pb.ClearRequest) (*pb.ClearResponse, error) {
	return nil, errors.New("not implemented")
}

func (s TestServer) Events(request *pb.EventRequest, server pb.MapService_EventsServer) error {
	s.incrementIndex()
	s.streams[s.index] = &TestStream{

	}
	return nil
}

func serve(l *bufconn.Listener, c <-chan struct{}) {
	s := grpc.NewServer()
	pb.RegisterMapServiceServer(s, NewTestServer())
	go func() {
		if err := s.Serve(l); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	go func() {
		for {
			select {
			case <-c:
				s.Stop()
				return
			}
		}
	}()
}

func TestMap(t *testing.T) {
	l := bufconn.Listen(1024 * 1024)
	stop := make(chan struct{})
	serve(l, stop)

	f := func(c context.Context, s string) (net.Conn, error) {
		return l.Dial()
	}

	c, err := grpc.Dial("test", grpc.WithContextDialer(f), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	m, err := NewMap(c, "test", protocol.MultiRaft("test"))

	r, err := m.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Error(err)
	}

	r, err = m.Get(context.Background(), "foo")
	if err != nil {
		t.Error(err)
	}
	if r.Key != "foo" {
		t.Error("Key does not match")
	}
	if string(r.Value) != "bar" {
		t.Error("Value does not match")
	}

	defer c.Close()
	stop <- struct{}{}
}

func example() {
	client, err := grpc.Dial("localhost", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	m, err := NewMap(client, "foo", protocol.MultiRaft("test"))
	if err != nil {
		panic(err)
	}

	_, err = m.Put(context.TODO(), "foo", []byte("bar"))
	if err != nil {
		panic(err)
	}

	v, err := m.Get(context.TODO(), "foo")

	println("Key is", v.Key)
	println("Value is", string(v.Value))
	println("Version is", v.Version)

	c := make(chan *MapEvent)
	go m.Listen(context.TODO(), c)

	for {
		select {
		case e := <-c:
			println(e.Key)
		}
	}
}
