package set

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/atomix/set"
	"github.com/golang/protobuf/ptypes/duration"
)

type SessionHandler struct {
	client pb.SetServiceClient
}

func (m *SessionHandler) Create(ctx context.Context, s *session.Session) error {
	request := &pb.CreateRequest{
		Header: s.GetHeader(),
		Timeout: &duration.Duration{
			Seconds: int64(s.Timeout.Seconds()),
			Nanos:   int32(s.Timeout.Nanoseconds()),
		},
	}
	response, err := m.client.Create(ctx, request)
	if err != nil {
		return err
	}
	s.UpdateHeader(response.Header)
	return nil
}

func (m *SessionHandler) KeepAlive(ctx context.Context, s *session.Session) error {
	request := &pb.KeepAliveRequest{
		Header: s.GetHeader(),
	}

	response, err := m.client.KeepAlive(ctx, request)
	if err != nil {
		return err
	}
	s.UpdateHeader(response.Header)
	return nil
}

func (m *SessionHandler) Close(ctx context.Context, s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetHeader(),
	}
	_, err := m.client.Close(ctx, request)
	return err
}

func (m *SessionHandler) Delete(ctx context.Context, s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetHeader(),
		Delete: true,
	}
	_, err := m.client.Close(ctx, request)
	return err
}
