package lock

import (
	"context"
	"github.com/atomix/atomix-go/pkg/client/protocol"
	"github.com/atomix/atomix-go/pkg/client/test"
	pb "github.com/atomix/atomix-go/proto/lock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"time"
)

// NewTestServer creates a new server for managing sessions
func NewTestServer() *TestServer {
	return &TestServer{
		TestServer: test.NewTestServer(),
		queue:      []*LockAttempt{},
	}
}

type TestServer struct {
	*test.TestServer
	lock  *LockAttempt
	queue []*LockAttempt
}

type LockAttempt struct {
	version uint64
	request *pb.LockRequest
	time    time.Time
	c       chan<- bool
}

func (s *TestServer) Create(ctx context.Context, request *pb.CreateRequest) (*pb.CreateResponse, error) {
	headers, err := s.CreateHeaders(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.CreateResponse{
		Headers: headers,
	}, nil
}

func (s *TestServer) KeepAlive(ctx context.Context, request *pb.KeepAliveRequest) (*pb.KeepAliveResponse, error) {
	headers, err := s.KeepAliveHeaders(ctx, request.Headers)
	if err != nil {
		return nil, err
	}
	return &pb.KeepAliveResponse{
		Headers: headers,
	}, nil
}

func (s *TestServer) Close(ctx context.Context, request *pb.CloseRequest) (*pb.CloseResponse, error) {
	err := s.CloseHeaders(ctx, request.Headers)
	if err != nil {
		return nil, err
	}
	return &pb.CloseResponse{}, nil
}

func (s *TestServer) Lock(ctx context.Context, request *pb.LockRequest) (*pb.LockResponse, error) {
	index := s.IncrementIndex()

	session, err := s.GetSession(request.Headers.SessionId)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Headers.Headers[0].SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	if s.lock != nil {
		c := make(chan bool)
		attempt := &LockAttempt{
			request: request,
			c:       c,
			time:    time.Now(),
		}
		s.queue = append(s.queue, attempt)
		succeeded := <-c

		headers, err := session.NewResponseHeaders()
		if err != nil {
			return nil, err
		}

		if succeeded {
			attempt.version = s.Index
			s.lock = attempt
			return &pb.LockResponse{
				Headers: headers,
				Version: s.Index,
			}, nil
		} else {
			return &pb.LockResponse{
				Headers: headers,
				Version: 0,
			}, nil
		}
	} else {
		headers, err := session.NewResponseHeaders()
		if err != nil {
			return nil, err
		}

		s.lock = &LockAttempt{
			version: index,
			request: request,
		}
		return &pb.LockResponse{
			Headers: headers,
			Version: index,
		}, nil
	}
}

func (s *TestServer) Unlock(ctx context.Context, request *pb.UnlockRequest) (*pb.UnlockResponse, error) {
	s.IncrementIndex()

	session, err := s.GetSession(request.Headers.SessionId)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Headers.Headers[0].SequenceNumber
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	headers, err := session.NewResponseHeaders()
	if err != nil {
		return nil, err
	}

	if s.lock == nil || (request.Version != 0 && s.lock.version != request.Version) {
		return &pb.UnlockResponse{
			Headers:  headers,
			Unlocked: false,
		}, nil
	}

	s.lock = nil

	if len(s.queue) > 0 {
		attempt := s.queue[0]
		s.queue = s.queue[1:]

		if attempt.request.Timeout != nil && (attempt.request.Timeout.Seconds > 0 || attempt.request.Timeout.Nanos > 0) {
			t := time.Duration(attempt.request.Timeout.Seconds + int64(attempt.request.Timeout.Nanos))
			d := time.Now().Sub(attempt.time)
			if d > t {
				attempt.c <- false
			} else {
				attempt.c <- true
			}
		} else {
			attempt.c <- true
		}
	}

	return &pb.UnlockResponse{
		Headers:  headers,
		Unlocked: true,
	}, nil
}

func (s *TestServer) IsLocked(ctx context.Context, request *pb.IsLockedRequest) (*pb.IsLockedResponse, error) {
	session, err := s.GetSession(request.Headers.SessionId)
	if err != nil {
		return nil, err
	}

	headers, err := session.NewResponseHeaders()
	if err != nil {
		return nil, err
	}

	if s.lock == nil {
		return &pb.IsLockedResponse{
			Headers:  headers,
			IsLocked: false,
		}, nil
	} else if request.Version > 0 && s.lock.version != request.Version {
		return &pb.IsLockedResponse{
			Headers:  headers,
			IsLocked: false,
		}, nil
	} else {
		return &pb.IsLockedResponse{
			Headers:  headers,
			IsLocked: true,
		}, nil
	}
}

func TestLock(t *testing.T) {
	conn, server := test.StartTestServer(func(server *grpc.Server) {
		pb.RegisterLockServiceServer(server, NewTestServer())
	})

	l1, err := NewLock(conn, "test", protocol.MultiRaft("test"))
	assert.NoError(t, err)

	l2, err := NewLock(conn, "test", protocol.MultiRaft("test"))
	assert.NoError(t, err)

	v1, err := l1.Lock(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, 0, v1)

	locked, err := l1.IsLocked(context.Background())
	assert.NoError(t, err)
	assert.True(t, locked)

	locked, err = l2.IsLocked(context.Background())
	assert.NoError(t, err)
	assert.True(t, locked)

	var v2 uint64
	c := make(chan struct{})
	go func() {
		v2, err = l2.Lock(context.Background())
		assert.NoError(t, err)
		c <- struct{}{}
	}()

	success, err := l1.Unlock(context.Background())
	assert.NoError(t, err)
	assert.True(t, success)

	<-c

	assert.NotEqual(t, v1, v2)

	locked, err = l1.IsLocked(context.Background())
	assert.NoError(t, err)
	assert.True(t, locked)

	locked, err = l1.IsLocked(context.Background(), IsLockedVersion(v1))
	assert.NoError(t, err)
	assert.False(t, locked)

	locked, err = l1.IsLocked(context.Background(), IsLockedVersion(v2))
	assert.NoError(t, err)
	assert.True(t, locked)

	test.StopTestServer(server)
}
