package lock

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/atomix/lock"
	"github.com/golang/protobuf/ptypes/duration"
)

type SessionHandler struct {
	session.Handler
	client pb.LockServiceClient
}

func (h *SessionHandler) Create(s *session.Session) error {
	request := &pb.CreateRequest{
		Header: s.GetHeader(),
		Timeout: &duration.Duration{
			Seconds: int64(s.Timeout.Seconds()),
			Nanos:   int32(s.Timeout.Nanoseconds()),
		},
	}

	response, err := h.client.Create(context.Background(), request)
	if err != nil {
		return err
	}
	s.UpdateHeader(response.Header)
	return nil
}

func (h *SessionHandler) KeepAlive(s *session.Session) error {
	request := &pb.KeepAliveRequest{
		Header: s.GetHeader(),
	}

	if _, err := h.client.KeepAlive(context.Background(), request); err != nil {
		return err
	}
	return nil
}

func (h *SessionHandler) Close(s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetHeader(),
	}

	if _, err := h.client.Close(context.Background(), request); err != nil {
		return err
	}
	return nil
}
