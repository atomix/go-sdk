package election

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	pb "github.com/atomix/atomix-go-client/proto/atomix/election"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

func NewElection(namespace string, name string, candidate string, partitions []*grpc.ClientConn, opts ...session.Option) (*Election, error) {
	i, err := util.GetPartitionIndex(name, len(partitions))
	if err != nil {
		return nil, err
	}

	client := pb.NewLeaderElectionServiceClient(partitions[i])
	session := session.NewSession(namespace, name, &SessionHandler{client: client}, opts...)
	if err := session.Start(); err != nil {
		return nil, err
	}

	return &Election{
		client:      client,
		session:     session,
		candidateId: candidate,
	}, nil
}

type Election struct {
	Interface
	client      pb.LeaderElectionServiceClient
	session     *session.Session
	candidateId string
}

func (e *Election) GetTerm(ctx context.Context) (*Term, error) {
	request := &pb.GetLeadershipRequest{
		Header: e.session.GetHeader(),
	}

	response, err := e.client.GetLeadership(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.UpdateHeader(response.Header)
	return &Term{
		Term:       response.Term,
		Leader:     response.Leader,
		Candidates: response.Candidates,
	}, nil
}

func (e *Election) Enter(ctx context.Context) (*Term, error) {
	request := &pb.EnterRequest{
		Header:      e.session.NextHeader(),
		CandidateId: e.candidateId,
	}

	response, err := e.client.Enter(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.UpdateHeader(response.Header)
	return &Term{
		Term:       response.Term,
		Leader:     response.Leader,
		Candidates: response.Candidates,
	}, nil
}

func (e *Election) Leave(ctx context.Context) error {
	request := &pb.WithdrawRequest{
		Header:      e.session.NextHeader(),
		CandidateId: e.candidateId,
	}

	response, err := e.client.Withdraw(ctx, request)
	if err != nil {
		return err
	}

	e.session.UpdateHeader(response.Header)
	return nil
}

func (e *Election) Anoint(ctx context.Context, id string) (bool, error) {
	request := &pb.AnointRequest{
		Header:      e.session.NextHeader(),
		CandidateId: id,
	}

	response, err := e.client.Anoint(ctx, request)
	if err != nil {
		return false, err
	}

	e.session.UpdateHeader(response.Header)
	return response.Succeeded, nil
}

func (e *Election) Promote(ctx context.Context, id string) (bool, error) {
	request := &pb.PromoteRequest{
		Header:      e.session.NextHeader(),
		CandidateId: id,
	}

	response, err := e.client.Promote(ctx, request)
	if err != nil {
		return false, err
	}

	e.session.UpdateHeader(response.Header)
	return response.Succeeded, nil
}

func (e *Election) Evict(ctx context.Context, id string) (bool, error) {
	request := &pb.EvictRequest{
		Header:      e.session.NextHeader(),
		CandidateId: id,
	}

	response, err := e.client.Evict(ctx, request)
	if err != nil {
		return false, err
	}

	e.session.UpdateHeader(response.Header)
	return response.Succeeded, nil
}

func (e *Election) Listen(ctx context.Context, c chan<- *ElectionEvent) error {
	request := &pb.EventRequest{
		Header: e.session.NextHeader(),
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

			if e.session.ValidStream(response.Header) {
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

func (e *Election) Close() error {
	return e.session.Stop()
}
