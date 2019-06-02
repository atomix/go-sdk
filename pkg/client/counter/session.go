package counter

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/atomix/counter"
)

type SessionHandler struct {
	session.Handler
	client pb.CounterServiceClient
}

func (m *SessionHandler) Create(s *session.Session) error {
	request := &pb.CreateRequest{
		Header: s.GetHeader(),
	}
	response, err := m.client.Create(context.Background(), request)
	if err != nil {
		return err
	}
	s.UpdateHeader(response.Header)
	return nil
}

func (m *SessionHandler) KeepAlive(s *session.Session) error {
	return nil
}

func (m *SessionHandler) close(s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetHeader(),
	}
	_, err := m.client.Close(context.Background(), request)
	return err
}
