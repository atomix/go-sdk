package set

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/atomix/set"
	"github.com/golang/protobuf/ptypes"
)

type SessionHandler struct {
	client pb.SetServiceClient
}

func (m *SessionHandler) Create(ctx context.Context, s *session.Session) error {
	request := &pb.CreateRequest{
		Header: s.GetState(),
		Timeout: ptypes.DurationProto(s.Timeout),
	}
	response, err := m.client.Create(ctx, request)
	if err != nil {
		return err
	}
	s.RecordResponse(request.Header, response.Header)
	return nil
}

func (m *SessionHandler) KeepAlive(ctx context.Context, s *session.Session) error {
	request := &pb.KeepAliveRequest{
		Header: s.GetState(),
	}

	_, err := m.client.KeepAlive(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (m *SessionHandler) Close(ctx context.Context, s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetState(),
	}
	_, err := m.client.Close(ctx, request)
	return err
}

func (m *SessionHandler) Delete(ctx context.Context, s *session.Session) error {
	request := &pb.CloseRequest{
		Header: s.GetState(),
		Delete: true,
	}
	_, err := m.client.Close(ctx, request)
	return err
}
