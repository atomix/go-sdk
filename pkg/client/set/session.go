package set

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/atomix/set"
	"github.com/golang/protobuf/ptypes/duration"
)

type SessionHandler struct {
	session.Handler
	client pb.SetServiceClient
}

func (m *SessionHandler) Create(s *session.Session) error {
	request := &pb.CreateRequest{
		Header: s.GetHeader(),
		Timeout: &duration.Duration{
			Seconds: int64(s.Timeout.Seconds()),
			Nanos:   int32(s.Timeout.Nanoseconds()),
		},
	}
	response, err := m.client.Create(context.Background(), request)
	if err != nil {
		return err
	}
	s.UpdateHeader(response.Header)
	return nil
}

func (m *SessionHandler) KeepAlive(s *session.Session) error {
	request := &pb.KeepAliveRequest{
		Header: s.GetHeader(),
	}

	response, err := m.client.KeepAlive(context.Background(), request)
	if err != nil {
		return err
	}
	s.UpdateHeader(response.Header)
	return nil
}

func (m *SessionHandler) close(s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetHeader(),
	}

	_, err := m.client.Close(context.Background(), request)
	return err
}
