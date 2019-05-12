package _map

import (
	"context"
	"github.com/atomix/atomix-go/pkg/client/protocol"
	"github.com/atomix/atomix-go/pkg/client/session"
	"github.com/atomix/atomix-go/proto/headers"
	pb "github.com/atomix/atomix-go/proto/map"
	"github.com/atomix/atomix-go/proto/protocol"
	"github.com/golang/protobuf/ptypes/duration"
)

func newSession(c pb.MapServiceClient, name string, protocol *protocol.Protocol, opts ...session.Option) *Session {
	return &Session{
		client: c,
		name: name,
		mapId: newMapId(name, protocol),
		Session: session.NewSession(opts...),
	}
}

type Session struct {
	*session.Session
	client pb.MapServiceClient
	name string
	mapId *pb.MapId
}

func newMapId(name string, protocol *protocol.Protocol) *pb.MapId {
	if protocol.MultiRaft != nil {
		return &pb.MapId{
			Name: name,
			Proto: &pb.MapId_Raft{
				Raft: &atomix_protocol.MultiRaftProtocol{
					Group: protocol.MultiRaft.Group,
				},
			},
		}
	} else if protocol.MultiPrimary != nil {
		return &pb.MapId{
			Name: name,
			Proto: &pb.MapId_MultiPrimary{
				MultiPrimary: &atomix_protocol.MultiPrimaryProtocol{
					Group: protocol.MultiPrimary.Group,
				},
			},
		}
	} else if protocol.MultiLog != nil {
		return &pb.MapId{
			Name: name,
			Proto: &pb.MapId_Log{
				Log: &atomix_protocol.DistributedLogProtocol{
					Group: protocol.MultiLog.Group,
				},
			},
		}
	}
	return nil
}

func (m *Session) Connect() error {
	request := &pb.CreateRequest{
		Id: m.mapId,
		Timeout: &duration.Duration{
			Seconds: int64(m.Timeout.Seconds()),
			Nanos: int32(m.Timeout.Nanoseconds()),
		},
	}

	response, err := m.client.Create(context.Background(), request)
	if err != nil {
		return err
	}

	m.Start(response.Headers)
	return nil
}

func (m *Session) keepAlive() error {
	request := &pb.KeepAliveRequest{
		Id: m.mapId,
		Headers: m.Headers.Session(),
	}

	if _, err := m.client.KeepAlive(context.Background(), request); err != nil {
		return err
	}
	return nil
}

func (m *Session) Close() error {
	m.Stop()

	request := &pb.CloseRequest{
		Id: m.mapId,
		Headers: &headers.SessionHeaders{
			SessionId: m.Id,
		},
	}

	if _, err := m.client.Close(context.Background(), request); err != nil {
		return err
	}
	return nil
}
