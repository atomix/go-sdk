package election

import (
	"context"
	"github.com/atomix/atomix-go/pkg/client/protocol"
	"github.com/atomix/atomix-go/pkg/client/session"
	pb "github.com/atomix/atomix-go/proto/election"
	"github.com/atomix/atomix-go/proto/headers"
	"github.com/atomix/atomix-go/proto/protocol"
	"github.com/golang/protobuf/ptypes/duration"
)

func newSession(c pb.LeaderElectionServiceClient, name string, protocol *protocol.Protocol, opts ...session.Option) *Session {
	return &Session{
		client:     c,
		name:       name,
		electionId: newElectionId(name, protocol),
		Session:    session.NewSession(opts...),
	}
}

type Session struct {
	*session.Session
	client     pb.LeaderElectionServiceClient
	name       string
	electionId *pb.ElectionId
}

func newElectionId(name string, protocol *protocol.Protocol) *pb.ElectionId {
	if protocol.MultiRaft != nil {
		return &pb.ElectionId{
			Name: name,
			Proto: &pb.ElectionId_Raft{
				Raft: &atomix_protocol.MultiRaftProtocol{
					Group: protocol.MultiRaft.Group,
				},
			},
		}
	} else if protocol.MultiPrimary != nil {
		return &pb.ElectionId{
			Name: name,
			Proto: &pb.ElectionId_MultiPrimary{
				MultiPrimary: &atomix_protocol.MultiPrimaryProtocol{
					Group: protocol.MultiPrimary.Group,
				},
			},
		}
	} else if protocol.MultiLog != nil {
		return &pb.ElectionId{
			Name: name,
			Proto: &pb.ElectionId_Log{
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
		Id: m.electionId,
		Timeout: &duration.Duration{
			Seconds: int64(m.Timeout.Seconds()),
			Nanos:   int32(m.Timeout.Nanoseconds()),
		},
	}

	response, err := m.client.Create(context.Background(), request)
	if err != nil {
		return err
	}

	m.Start([]*headers.SessionHeader{response.Header})
	return nil
}

func (m *Session) keepAlive() error {
	request := &pb.KeepAliveRequest{
		Id:     m.electionId,
		Header: m.Headers.GetPartition("").GetSessionHeader(),
	}

	if _, err := m.client.KeepAlive(context.Background(), request); err != nil {
		return err
	}
	return nil
}

func (m *Session) Close() error {
	m.Stop()

	request := &pb.CloseRequest{
		Id:     m.electionId,
		Header: m.Headers.GetPartition("").GetSessionHeader(),
	}

	if _, err := m.client.Close(context.Background(), request); err != nil {
		return err
	}
	return nil
}
