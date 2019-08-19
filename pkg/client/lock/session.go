package lock

import (
	"context"
	api "github.com/atomix/atomix-api/proto/atomix/lock"
	"github.com/atomix/atomix-go-client/pkg/client/session"
)

type SessionHandler struct {
	client api.LockServiceClient
}

func (h *SessionHandler) Create(ctx context.Context, s *session.Session) error {
	request := &api.CreateRequest{
		Header:  s.GetState(),
		Timeout: &s.Timeout,
	}

	response, err := h.client.Create(ctx, request)
	if err != nil {
		return err
	}
	s.RecordResponse(request.Header, response.Header)
	return nil
}

func (h *SessionHandler) KeepAlive(ctx context.Context, s *session.Session) error {
	request := &api.KeepAliveRequest{
		Header: s.GetState(),
	}

	if _, err := h.client.KeepAlive(ctx, request); err != nil {
		return err
	}
	return nil
}

func (h *SessionHandler) Close(ctx context.Context, s *session.Session) error {
	request := &api.CloseRequest{
		Header: s.GetState(),
	}
	_, err := h.client.Close(ctx, request);
	return err
}

func (h *SessionHandler) Delete(ctx context.Context, s *session.Session) error {
	request := &api.CloseRequest{
		Header: s.GetState(),
		Delete: true,
	}
	_, err := h.client.Close(ctx, request);
	return err
}
