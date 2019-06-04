package _map

import (
	"context"
	"errors"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/atomix/map"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

func newPartition(ctx context.Context, conn *grpc.ClientConn, namespace string, name string, opts ...session.SessionOption) (Map, error) {
	client := pb.NewMapServiceClient(conn)
	sess, err := session.New(ctx, namespace, name, &SessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &mapPartition{
		client:  client,
		session: sess,
	}, nil
}

type mapPartition struct {
	client  pb.MapServiceClient
	session *session.Session
}

func (m *mapPartition) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*KeyValue, error) {
	request := &pb.PutRequest{
		Header: m.session.NextHeader(),
		Key:    key,
		Value:  value,
	}

	for i := range opts {
		opts[i].beforePut(request)
	}

	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterPut(response)
	}

	m.session.UpdateHeader(response.Header)

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

func (m *mapPartition) Get(ctx context.Context, key string, opts ...GetOption) (*KeyValue, error) {
	request := &pb.GetRequest{
		Header: m.session.GetHeader(),
		Key:    key,
	}

	for i := range opts {
		opts[i].beforeGet(request)
	}

	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterGet(response)
	}

	m.session.UpdateHeader(response.Header)

	if response.Version != 0 {
		return &KeyValue{
			Key:     key,
			Value:   response.Value,
			Version: response.Version,
		}, nil
	}
	return nil, nil
}

func (m *mapPartition) Remove(ctx context.Context, key string, opts ...RemoveOption) (*KeyValue, error) {
	request := &pb.RemoveRequest{
		Header: m.session.NextHeader(),
		Key:    key,
	}

	for i := range opts {
		opts[i].beforeRemove(request)
	}

	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterRemove(response)
	}

	m.session.UpdateHeader(response.Header)

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

func (m *mapPartition) Size(ctx context.Context) (int, error) {
	request := &pb.SizeRequest{
		Header: m.session.GetHeader(),
	}

	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	m.session.UpdateHeader(response.Header)
	return int(response.Size), nil
}

func (m *mapPartition) Clear(ctx context.Context) error {
	request := &pb.ClearRequest{
		Header: m.session.NextHeader(),
	}

	response, err := m.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	m.session.UpdateHeader(response.Header)
	return nil
}

func (m *mapPartition) Listen(ctx context.Context, c chan<- *MapEvent) error {
	request := &pb.EventRequest{
		Header: m.session.NextHeader(),
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

			if m.session.ValidStream(response.Header) {
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

func (m *mapPartition) Close() error {
	return m.session.Close()
}
