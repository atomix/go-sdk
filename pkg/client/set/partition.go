package set

import (
	"context"
	"errors"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/atomix/set"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

func newPartition(ctx context.Context, conn *grpc.ClientConn, name primitive.Name, opts ...session.SessionOption) (Set, error) {
	client := pb.NewSetServiceClient(conn)
	sess, err := session.New(ctx, name, &SessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &setPartition{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

type setPartition struct {
	name    primitive.Name
	client  pb.SetServiceClient
	session *session.Session
}

func (s *setPartition) Name() primitive.Name {
	return s.name
}

func (s *setPartition) Add(ctx context.Context, value string) (bool, error) {
	request := &pb.AddRequest{
		Header: s.session.NextRequest(),
		Values: []string{value},
	}

	response, err := s.client.Add(ctx, request)
	if err != nil {
		return false, err
	}

	s.session.RecordResponse(response.Header)

	if response.Status == pb.ResponseStatus_WRITE_LOCK {
		return false, errors.New("write lock failed")
	}
	return response.Added, nil
}

func (s *setPartition) Remove(ctx context.Context, value string) (bool, error) {
	request := &pb.RemoveRequest{
		Header: s.session.NextRequest(),
		Values: []string{value},
	}

	response, err := s.client.Remove(ctx, request)
	if err != nil {
		return false, err
	}

	s.session.RecordResponse(response.Header)

	if response.Status == pb.ResponseStatus_WRITE_LOCK {
		return false, errors.New("write lock failed")
	}
	return response.Removed, nil
}

func (s *setPartition) Contains(ctx context.Context, value string) (bool, error) {
	request := &pb.ContainsRequest{
		Header: s.session.GetRequest(),
		Values: []string{value},
	}

	response, err := s.client.Contains(ctx, request)
	if err != nil {
		return false, err
	}

	s.session.RecordResponse(response.Header)
	return response.Contains, nil
}

func (s *setPartition) Size(ctx context.Context) (int, error) {
	request := &pb.SizeRequest{
		Header: s.session.GetRequest(),
	}

	response, err := s.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	s.session.RecordResponse(response.Header)
	return int(response.Size), nil
}

func (s *setPartition) Clear(ctx context.Context) error {
	request := &pb.ClearRequest{
		Header: s.session.NextRequest(),
	}

	response, err := s.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	s.session.RecordResponse(response.Header)
	return nil
}

func (s *setPartition) Watch(ctx context.Context, c chan<- *SetEvent, opts ...WatchOption) error {
	request := &pb.EventRequest{
		Header: s.session.NextRequest(),
	}

	for _, opt := range opts {
		opt.beforeWatch(request)
	}

	events, err := s.client.Events(ctx, request)
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
			s.session.RecordResponse(response.Header)

			// Initialize the session stream if necessary.
			if stream == nil {
				stream = s.session.NewStream(response.Header.StreamId)
			}

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			var t SetEventType
			switch response.Type {
			case pb.EventResponse_ADDED:
				t = EVENT_ADDED
			case pb.EventResponse_REMOVED:
				t = EVENT_REMOVED
			}

			c <- &SetEvent{
				Type:  t,
				Value: response.Value,
			}
		}
	}()
	return nil
}

func (s *setPartition) Close() error {
	return s.session.Close()
}

func (s *setPartition) Delete() error {
	return s.session.Delete()
}
