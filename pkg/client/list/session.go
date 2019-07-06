package list

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/atomix/list"
	"github.com/golang/protobuf/ptypes/duration"
)

type SessionHandler struct {
	client pb.ListServiceClient
}

func (h *SessionHandler) Create(ctx context.Context, s *session.Session) error {
	request := &pb.CreateRequest{
		Header: s.GetHeader(),
		Timeout: &duration.Duration{
			Seconds: int64(s.Timeout.Seconds()),
			Nanos:   int32(s.Timeout.Nanoseconds()),
		},
	}

	response, err := h.client.Create(ctx, request)
	if err != nil {
		return err
	}
	s.UpdateHeader(response.Header)
	return nil
}

func (h *SessionHandler) KeepAlive(ctx context.Context, s *session.Session) error {
	request := &pb.KeepAliveRequest{
		Header: s.GetHeader(),
	}

	if _, err := h.client.KeepAlive(ctx, request); err != nil {
		return err
	}
	return nil
}

func (h *SessionHandler) Close(ctx context.Context, s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetHeader(),
	}
	_, err := h.client.Close(ctx, request);
	return err
}

func (h *SessionHandler) Delete(ctx context.Context, s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetHeader(),
		Delete: true,
	}
	_, err := h.client.Close(ctx, request);
	return err
}
