package election

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/protocol"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	pb "github.com/atomix/atomix-go-client/proto/election"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

func NewElection(conn *grpc.ClientConn, name string, protocol *protocol.Protocol, opts ...session.Option) (*Election, error) {
	c := pb.NewLeaderElectionServiceClient(conn)
	s := newSession(c, name, protocol, opts...)
	if err := s.Connect(); err != nil {
		return nil, err
	}

	return &Election{
		client:  c,
		session: s,
	}, nil
}

type Election struct {
	client  pb.LeaderElectionServiceClient
	session *Session
}

func (e *Election) GetTerm(ctx context.Context) (*Term, error) {
	partition := e.session.Headers.GetPartition("")

	request := &pb.GetLeadershipRequest{
		Id:     e.session.electionId,
		Header: partition.GetQueryHeader(),
	}

	response, err := e.client.GetLeadership(ctx, request)
	if err != nil {
		return nil, err
	}

	partition.UpdateHeader(response.Header)
	return &Term{
		Term:       response.Term,
		Leader:     response.Leader,
		Candidates: response.Candidates,
	}, nil
}

func (e *Election) Enter(ctx context.Context, id string) (*Term, error) {
	partition := e.session.Headers.GetPartition("")

	request := &pb.EnterRequest{
		Id:          e.session.electionId,
		Header:      partition.GetCommandHeader(),
		CandidateId: id,
	}

	response, err := e.client.Enter(ctx, request)
	if err != nil {
		return nil, err
	}

	partition.UpdateHeader(response.Header)
	return &Term{
		Term:       response.Term,
		Leader:     response.Leader,
		Candidates: response.Candidates,
	}, nil
}

func (e *Election) Withdraw(ctx context.Context, id string) error {
	partition := e.session.Headers.GetPartition("")

	request := &pb.WithdrawRequest{
		Id:          e.session.electionId,
		Header:      partition.GetCommandHeader(),
		CandidateId: id,
	}

	response, err := e.client.Withdraw(ctx, request)
	if err != nil {
		return err
	}

	partition.UpdateHeader(response.Header)
	return nil
}

func (e *Election) Anoint(ctx context.Context, id string) (bool, error) {
	partition := e.session.Headers.GetPartition("")

	request := &pb.AnointRequest{
		Id:          e.session.electionId,
		Header:      partition.GetCommandHeader(),
		CandidateId: id,
	}

	response, err := e.client.Anoint(ctx, request)
	if err != nil {
		return false, err
	}

	partition.UpdateHeader(response.Header)
	return response.Succeeded, nil
}

func (e *Election) Promote(ctx context.Context, id string) (bool, error) {
	partition := e.session.Headers.GetPartition("")

	request := &pb.PromoteRequest{
		Id:          e.session.electionId,
		Header:      partition.GetCommandHeader(),
		CandidateId: id,
	}

	response, err := e.client.Promote(ctx, request)
	if err != nil {
		return false, err
	}

	partition.UpdateHeader(response.Header)
	return response.Succeeded, nil
}

func (e *Election) Evict(ctx context.Context, id string) (bool, error) {
	partition := e.session.Headers.GetPartition("")

	request := &pb.EvictRequest{
		Id:          e.session.electionId,
		Header:      partition.GetCommandHeader(),
		CandidateId: id,
	}

	response, err := e.client.Evict(ctx, request)
	if err != nil {
		return false, err
	}

	partition.UpdateHeader(response.Header)
	return response.Succeeded, nil
}

func (e *Election) Listen(ctx context.Context, c chan<- *ElectionEvent) error {
	partition := e.session.Headers.GetPartition("")

	request := &pb.EventRequest{
		Id:     e.session.electionId,
		Header: partition.GetCommandHeader(),
	}
	events, err := e.client.Events(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		for {
			response, err := events.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				glog.Error("Failed to receive event stream", err)
			}

			if e.session.Headers.Validate(response.Header) {
				c <- &ElectionEvent{
					Type: EVENT_CHANGED,
					Term: Term{
						Term:       response.Term,
						Leader:     response.Leader,
						Candidates: response.Candidates,
					},
				}
			}
		}
	}()
	return nil
}

type Term struct {
	Term       uint64
	Leader     string
	Candidates []string
}

type ElectionEventType string

const (
	EVENT_CHANGED ElectionEventType = "changed"
)

type ElectionEvent struct {
	Type ElectionEventType
	Term Term
}

func (e *Election) Close() error {
	return e.session.Close()
}
