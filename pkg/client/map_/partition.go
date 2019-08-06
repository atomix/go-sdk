package map_

import (
	"context"
	"errors"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/atomix/map"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

func newPartition(ctx context.Context, conn *grpc.ClientConn, name primitive.Name, opts ...session.SessionOption) (Map, error) {
	client := pb.NewMapServiceClient(conn)
	sess, err := session.New(ctx, name, &SessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &mapPartition{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

type mapPartition struct {
	name    primitive.Name
	client  pb.MapServiceClient
	session *session.Session
}

func (m *mapPartition) Name() primitive.Name {
	return m.name
}

func (m *mapPartition) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*KeyValue, error) {
	request := &pb.PutRequest{
		Header: m.session.NextRequest(),
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

	m.session.RecordResponse(response.Header)

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
		Header: m.session.GetRequest(),
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

	m.session.RecordResponse(response.Header)

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
		Header: m.session.NextRequest(),
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

	m.session.RecordResponse(response.Header)

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
		Header: m.session.GetRequest(),
	}

	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	m.session.RecordResponse(response.Header)
	return int(response.Size), nil
}

func (m *mapPartition) Clear(ctx context.Context) error {
	request := &pb.ClearRequest{
		Header: m.session.NextRequest(),
	}

	response, err := m.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	m.session.RecordResponse(response.Header)
	return nil
}

func (m *mapPartition) Entries(ctx context.Context, ch chan<- *KeyValue) error {
	request := &pb.EntriesRequest{
		Header: m.session.GetRequest(),
	}
	entries, err := m.client.Entries(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		for {
			response, err := entries.Recv()
			if err == io.EOF {
				close(ch)
				break
			}

			if err != nil {
				glog.Error("Failed to receive entry stream", err)
			}

			// Record the response header
			m.session.RecordResponse(response.Header)

			ch <- &KeyValue{
				Key:     response.Key,
				Value:   response.Value,
				Version: response.Version,
			}
		}
	}()
	return nil
}

func (m *mapPartition) Watch(ctx context.Context, c chan<- *MapEvent, opts ...WatchOption) error {
	request := &pb.EventRequest{
		Header: m.session.NextRequest(),
	}

	for _, opt := range opts {
		opt.beforeWatch(request)
	}

	events, err := m.client.Events(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		var stream *session.Stream
		for {
			response, err := events.Recv()
			if err == io.EOF {
				if stream != nil {
					stream.Close()
				}
				break
			}

			if err != nil {
				glog.Error("Failed to receive event stream", err)
			}

			for _, opt := range opts {
				opt.afterWatch(response)
			}

			// Record the response header
			m.session.RecordResponse(response.Header)

			// Initialize the session stream if necessary.
			if stream == nil {
				stream = m.session.NewStream(response.Header.StreamId)
			}

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			switch response.Type {
			case pb.EventResponse_NONE:
				c <- &MapEvent{
					Type:    EventNone,
					Key:     response.Key,
					Value:   response.NewValue,
					Version: response.NewVersion,
				}
			case pb.EventResponse_INSERTED:
				c <- &MapEvent{
					Type:    EventInserted,
					Key:     response.Key,
					Value:   response.NewValue,
					Version: response.NewVersion,
				}
			case pb.EventResponse_UPDATED:
				c <- &MapEvent{
					Type:    EventUpdated,
					Key:     response.Key,
					Value:   response.NewValue,
					Version: response.NewVersion,
				}
			case pb.EventResponse_REMOVED:
				c <- &MapEvent{
					Type:    EventRemoved,
					Key:     response.Key,
					Value:   response.OldValue,
					Version: response.OldVersion,
				}
			}
		}
	}()
	return nil
}

func (m *mapPartition) Close() error {
	return m.session.Close()
}

func (m *mapPartition) Delete() error {
	return m.session.Delete()
}
