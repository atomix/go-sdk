package election

import (
	"context"
	"encoding/base64"
	api "github.com/atomix/atomix-api/proto/atomix/election"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
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
	Id() string
	GetTerm(ctx context.Context) (*Term, error)
	Enter(ctx context.Context) (*Term, error)
	Leave(ctx context.Context) error
	Anoint(ctx context.Context, id string) (bool, error)
	Promote(ctx context.Context, id string) (bool, error)
	Evict(ctx context.Context, id string) (bool, error)
	Watch(ctx context.Context, c chan<- *ElectionEvent) error
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

func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.SessionOption) (Election, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}

	client := api.NewLeaderElectionServiceClient(partitions[i])
	sess, err := session.New(ctx, name, &SessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}

	nodeId := uuid.NodeID()
	candidate := base64.StdEncoding.EncodeToString(nodeId)
	return &election{
		name:    name,
		client:  client,
		session: sess,
		id:      candidate,
	}, nil
}

type election struct {
	name    primitive.Name
	client  api.LeaderElectionServiceClient
	session *session.Session
	id      string
}

func (e *election) Name() primitive.Name {
	return e.name
}

func (e *election) Id() string {
	return e.id
}

func (e *election) GetTerm(ctx context.Context) (*Term, error) {
	request := &api.GetLeadershipRequest{
		Header: e.session.GetRequest(),
	}

	response, err := e.client.GetLeadership(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return &Term{
		Term:       response.Term,
		Leader:     response.Leader,
		Candidates: response.Candidates,
	}, nil
}

func (e *election) Enter(ctx context.Context) (*Term, error) {
	request := &api.EnterRequest{
		Header:      e.session.NextRequest(),
		CandidateId: e.id,
	}

	response, err := e.client.Enter(ctx, request)
	if err != nil {
		return nil, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return &Term{
		Term:       response.Term,
		Leader:     response.Leader,
		Candidates: response.Candidates,
	}, nil
}

func (e *election) Leave(ctx context.Context) error {
	request := &api.WithdrawRequest{
		Header:      e.session.NextRequest(),
		CandidateID: e.id,
	}

	response, err := e.client.Withdraw(ctx, request)
	if err != nil {
		return err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return nil
}

func (e *election) Anoint(ctx context.Context, id string) (bool, error) {
	request := &api.AnointRequest{
		Header:      e.session.NextRequest(),
		CandidateID: id,
	}

	response, err := e.client.Anoint(ctx, request)
	if err != nil {
		return false, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return response.Succeeded, nil
}

func (e *election) Promote(ctx context.Context, id string) (bool, error) {
	request := &api.PromoteRequest{
		Header:      e.session.NextRequest(),
		CandidateID: id,
	}

	response, err := e.client.Promote(ctx, request)
	if err != nil {
		return false, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return response.Succeeded, nil
}

func (e *election) Evict(ctx context.Context, id string) (bool, error) {
	request := &api.EvictRequest{
		Header:      e.session.NextRequest(),
		CandidateID: id,
	}

	response, err := e.client.Evict(ctx, request)
	if err != nil {
		return false, err
	}

	e.session.RecordResponse(request.Header, response.Header)
	return response.Succeeded, nil
}

func (e *election) Watch(ctx context.Context, ch chan<- *ElectionEvent) error {
	request := &api.EventRequest{
		Header: e.session.NextRequest(),
	}
	events, err := e.client.Events(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		defer close(ch)
		var stream *session.Stream
		for {
			response, err := events.Recv()
			if err == io.EOF {
				if stream != nil {
					stream.Close()
				}
				break
			}

			if err != nil {
				glog.Error("Failed to receive event stream", err)
				break
			}

			// Record the response header
			e.session.RecordResponse(request.Header, response.Header)

			// Initialize the session stream if necessary.
			if stream == nil {
				stream = e.session.NewStream(response.Header.StreamID)
			}

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			ch <- &ElectionEvent{
				Type: EVENT_CHANGED,
				Term: Term{
					Term:       response.Term,
					Leader:     response.Leader,
					Candidates: response.Candidates,
				},
			}
		}
	}()
	return nil
}

func (e *election) Close() error {
	return e.session.Close()
}

func (e *election) Delete() error {
	return e.session.Delete()
}
