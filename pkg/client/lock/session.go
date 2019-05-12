package lock

import (
	"context"
	"github.com/atomix/atomix-go/pkg/client/protocol"
	"github.com/atomix/atomix-go/pkg/client/session"
	"github.com/atomix/atomix-go/proto/headers"
	pb "github.com/atomix/atomix-go/proto/lock"
	"github.com/atomix/atomix-go/proto/protocol"
	"github.com/golang/protobuf/ptypes/duration"
)

func newSession(c pb.LockServiceClient, name string, protocol *protocol.Protocol, opts ...session.Option) *Session {
	return &Session{
		client: c,
		name: name,
		lockId: newLockId(name, protocol),
		Session: session.NewSession(opts...),
	}
}

type Session struct {
	*session.Session
	client pb.LockServiceClient
	name   string
	lockId *pb.LockId
}

func newLockId(name string, protocol *protocol.Protocol) *pb.LockId {
	if protocol.MultiRaft != nil {
		return &pb.LockId{
			Name: name,
			Proto: &pb.LockId_Raft{
				Raft: &atomix_protocol.MultiRaftProtocol{
					Group: protocol.MultiRaft.Group,
				},
			},
		}
	} else if protocol.MultiPrimary != nil {
		return &pb.LockId{
			Name: name,
			Proto: &pb.LockId_MultiPrimary{
				MultiPrimary: &atomix_protocol.MultiPrimaryProtocol{
					Group: protocol.MultiPrimary.Group,
				},
			},
		}
	} else if protocol.MultiLog != nil {
		return &pb.LockId{
			Name: name,
			Proto: &pb.LockId_Log{
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
		Id: m.lockId,
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
		Id: m.lockId,
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
		Id: m.lockId,
		Headers: &headers.SessionHeaders{
			SessionId: m.Id,
		},
	}

	if _, err := m.client.Close(context.Background(), request); err != nil {
		return err
	}
	return nil
}
