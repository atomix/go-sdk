package counter

import (
	"context"
	api "github.com/atomix/atomix-api/proto/atomix/counter"
	"github.com/atomix/atomix-go-client/pkg/client/session"
)

type SessionHandler struct {
	client api.CounterServiceClient
}

func (m *SessionHandler) Create(ctx context.Context, s *session.Session) error {
	request := &api.CreateRequest{
		Header: s.GetState(),
	}
	_, err := m.client.Create(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (m *SessionHandler) KeepAlive(ctx context.Context, s *session.Session) error {
	return nil
}

func (m *SessionHandler) Close(ctx context.Context, s *session.Session) error {
	request := &api.CloseRequest{
		Header: s.GetState(),
	}
	_, err := m.client.Close(ctx, request)
	return err
}

func (m *SessionHandler) Delete(ctx context.Context, s *session.Session) error {
	request := &api.CloseRequest{
		Header: s.GetState(),
		Delete: true,
	}
	_, err := m.client.Close(ctx, request)
	return err
}
