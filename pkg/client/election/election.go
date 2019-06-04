package election

import (
	"context"
	"encoding/base64"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	pb "github.com/atomix/atomix-go-client/proto/atomix/election"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"io"
)

type ElectionClient interface {
	GetElection(ctx context.Context, name string, opts ...session.SessionOption) (Election, error)
}

// Election is the interface for the leader election primitive
type Election interface {
	primitive.Primitive
	GetTerm(ctx context.Context) (*Term, error)
	Enter(ctx context.Context) (*Term, error)
	Leave(ctx context.Context) error
	Anoint(ctx context.Context, id string) (bool, error)
	Promote(ctx context.Context, id string) (bool, error)
	Evict(ctx context.Context, id string) (bool, error)
	Listen(ctx context.Context, c chan<- *ElectionEvent) error
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

func New(ctx context.Context, namespace string, name string, partitions []*grpc.ClientConn, opts ...session.SessionOption) (Election, error) {
	i, err := util.GetPartitionIndex(name, len(partitions))
	if err != nil {
		return nil, err
	}

	client := pb.NewLeaderElectionServiceClient(partitions[i])
	sess, err := session.New(ctx, namespace, name, &SessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}

	nodeId := uuid.NodeID()
	candidate := base64.StdEncoding.EncodeToString(nodeId)
	return &election{
		client:      client,
		session:     sess,
		candidateId: candidate,
	}, nil
}

type election struct {
	client      pb.LeaderElectionServiceClient
	session     *session.Session
	candidateId string
}

func (e *election) GetTerm(ctx context.Context) (*Term, error) {
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

func (e *election) Enter(ctx context.Context) (*Term, error) {
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

func (e *election) Leave(ctx context.Context) error {
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

func (e *election) Anoint(ctx context.Context, id string) (bool, error) {
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

func (e *election) Promote(ctx context.Context, id string) (bool, error) {
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

func (e *election) Evict(ctx context.Context, id string) (bool, error) {
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

func (e *election) Listen(ctx context.Context, c chan<- *ElectionEvent) error {
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

func (e *election) Close() error {
	return e.session.Stop()
}
