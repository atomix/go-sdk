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
		Header: s.session.NextHeader(),
		Values: []string{value},
	}

	response, err := s.client.Add(ctx, request)
	if err != nil {
		return false, err
	}

	s.session.UpdateHeader(response.Header)

	if response.Status == pb.ResponseStatus_WRITE_LOCK {
		return false, errors.New("write lock failed")
	}
	return response.Added, nil
}

func (s *setPartition) Remove(ctx context.Context, value string) (bool, error) {
	request := &pb.RemoveRequest{
		Header: s.session.NextHeader(),
		Values: []string{value},
	}

	response, err := s.client.Remove(ctx, request)
	if err != nil {
		return false, err
	}

	s.session.UpdateHeader(response.Header)

	if response.Status == pb.ResponseStatus_WRITE_LOCK {
		return false, errors.New("write lock failed")
	}
	return response.Removed, nil
}

func (s *setPartition) Contains(ctx context.Context, value string) (bool, error) {
	request := &pb.ContainsRequest{
		Header: s.session.NextHeader(),
		Values: []string{value},
	}

	response, err := s.client.Contains(ctx, request)
	if err != nil {
		return false, err
	}

	s.session.UpdateHeader(response.Header)
	return response.Contains, nil
}

func (s *setPartition) Size(ctx context.Context) (int, error) {
	request := &pb.SizeRequest{
		Header: s.session.GetHeader(),
	}

	response, err := s.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	s.session.UpdateHeader(response.Header)
	return int(response.Size), nil
}

func (s *setPartition) Clear(ctx context.Context) error {
	request := &pb.ClearRequest{
		Header: s.session.NextHeader(),
	}

	response, err := s.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	s.session.UpdateHeader(response.Header)
	return nil
}

func (s *setPartition) Listen(ctx context.Context, c chan<- *SetEvent) error {
	request := &pb.EventRequest{
		Header: s.session.NextHeader(),
	}
	events, err := s.client.Listen(ctx, request)
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

			var t SetEventType
			switch response.Type {
			case pb.EventResponse_ADDED:
				t = EVENT_ADDED
			case pb.EventResponse_REMOVED:
				t = EVENT_REMOVED
			}

			// If no stream headers are provided by the server, immediately complete the event.
			if len(response.Header.Streams) == 0 {
				c <- &SetEvent{
					Type:  t,
					Value: response.Value,
				}
			} else {
				// Wait for the stream to advanced at least to the responses.
				stream := response.Header.Streams[0]
				_, ok := <-s.session.WaitStream(stream)
				if ok {
					c <- &SetEvent{
						Type:  t,
						Value: response.Value,
					}
				}
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
