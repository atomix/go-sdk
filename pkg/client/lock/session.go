package lock

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/atomix/lock"
	"github.com/golang/protobuf/ptypes"
)

type SessionHandler struct {
	client pb.LockServiceClient
}

func (h *SessionHandler) Create(ctx context.Context, s *session.Session) error {
	request := &pb.CreateRequest{
		Header: s.GetRequest(),
		Timeout: ptypes.DurationProto(s.Timeout),
	}

	response, err := h.client.Create(ctx, request)
	if err != nil {
		return err
	}
	s.RecordResponse(response.Header)
	return nil
}

func (h *SessionHandler) KeepAlive(ctx context.Context, s *session.Session) error {
	request := &pb.KeepAliveRequest{
		Header: s.GetRequest(),
	}

	if _, err := h.client.KeepAlive(ctx, request); err != nil {
		return err
	}
	return nil
}

func (h *SessionHandler) Close(ctx context.Context, s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetRequest(),
	}
	_, err := h.client.Close(ctx, request);
	return err
}

func (h *SessionHandler) Delete(ctx context.Context, s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetRequest(),
		Delete: true,
	}
	_, err := h.client.Close(ctx, request);
	return err
}
