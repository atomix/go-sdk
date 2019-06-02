package _map

import (
	"context"
	"errors"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/proto/atomix/headers"
	pb "github.com/atomix/atomix-go-client/proto/atomix/map"
	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes/duration"
	"google.golang.org/grpc"
	"io"
)

func newSession(conn *grpc.ClientConn, namespace string, name string, opts ...session.Option) (*Session, error) {
	s := &Session{
		client:  pb.NewMapServiceClient(conn),
		Session: session.NewSession(namespace, name, opts...),
	}
	if err := s.Start(); err != nil {
		return nil, err
	}
	return s, nil
}

type Session struct {
	*session.Session
	mapInterface
	client pb.MapServiceClient
}

func (s *Session) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*KeyValue, error) {
	request := &pb.PutRequest{
		Header: s.NextHeader(),
		Key:    key,
		Value:  value,
	}

	for i := range opts {
		opts[i].before(request)
	}

	response, err := s.client.Put(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].after(response)
	}

	s.UpdateHeader(response.Header)

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

func (s *Session) Get(ctx context.Context, key string, opts ...GetOption) (*KeyValue, error) {
	request := &pb.GetRequest{
		Header: s.GetHeader(),
		Key:    key,
	}

	for i := range opts {
		opts[i].before(request)
	}

	response, err := s.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].after(response)
	}

	s.UpdateHeader(response.Header)

	if response.Version != 0 {
		return &KeyValue{
			Key:     key,
			Value:   response.Value,
			Version: response.Version,
		}, nil
	}
	return nil, nil
}

func (s *Session) Remove(ctx context.Context, key string, opts ...RemoveOption) (*KeyValue, error) {
	request := &pb.RemoveRequest{
		Header: s.NextHeader(),
		Key:    key,
	}

	for i := range opts {
		opts[i].before(request)
	}

	response, err := s.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].after(response)
	}

	s.UpdateHeader(response.Header)

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

func (s *Session) Size(ctx context.Context) (int, error) {
	request := &pb.SizeRequest{
		Header: s.GetHeader(),
	}

	response, err := s.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	s.UpdateHeader(response.Header)
	return int(response.Size), nil
}

func (s *Session) Clear(ctx context.Context) error {
	request := &pb.ClearRequest{
		Header: s.NextHeader(),
	}

	response, err := s.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	s.UpdateHeader(response.Header)
	return nil
}

func (s *Session) Listen(ctx context.Context, c chan<- *MapEvent) error {
	request := &pb.EventRequest{
		Header: s.NextHeader(),
	}
	events, err := s.client.Events(ctx, request)
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

			if s.ValidStream(response.Header) {
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

func (s *Session) connect(header *atomix_headers.RequestHeader) (*atomix_headers.ResponseHeader, error) {
	request := &pb.CreateRequest{
		Header: header,
		Timeout: &duration.Duration{
			Seconds: int64(s.Timeout.Seconds()),
			Nanos:   int32(s.Timeout.Nanoseconds()),
		},
	}
	response, err := s.client.Create(context.Background(), request)
	if err != nil {
		return nil, err
	}
	return response.Header, nil
}

func (s *Session) keepAlive(header *atomix_headers.RequestHeader) (*atomix_headers.ResponseHeader, error) {
	request := &pb.KeepAliveRequest{
		Header: header,
	}

	response, err := s.client.KeepAlive(context.Background(), request)
	if err != nil {
		return nil, err
	}
	return response.Header, nil
}

func (s *Session) close(header *atomix_headers.RequestHeader) (*atomix_headers.ResponseHeader, error) {
	request := &pb.CloseRequest{
		Header: header,
	}

	response, err := s.client.Close(context.Background(), request)
	if err != nil {
		return nil, err
	}
	return response.Header, nil
}
