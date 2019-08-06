package lock

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/test"
	pb "github.com/atomix/atomix-go-client/proto/atomix/lock"
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
	header, err := s.CreateHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.CreateResponse{
		Header: header,
	}, nil
}

func (s *TestServer) KeepAlive(ctx context.Context, request *pb.KeepAliveRequest) (*pb.KeepAliveResponse, error) {
	header, err := s.KeepAliveHeader(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	return &pb.KeepAliveResponse{
		Header: header,
	}, nil
}

func (s *TestServer) Close(ctx context.Context, request *pb.CloseRequest) (*pb.CloseResponse, error) {
	err := s.CloseHeader(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	return &pb.CloseResponse{}, nil
}

func (s *TestServer) Lock(ctx context.Context, request *pb.LockRequest) (*pb.LockResponse, error) {
	index := s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.RequestId
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

		header, err := session.NewResponseHeader()
		if err != nil {
			return nil, err
		}

		if succeeded {
			attempt.version = s.Index
			s.lock = attempt
			return &pb.LockResponse{
				Header:  header,
				Version: s.Index,
			}, nil
		} else {
			return &pb.LockResponse{
				Header:  header,
				Version: 0,
			}, nil
		}
	} else {
		header, err := session.NewResponseHeader()
		if err != nil {
			return nil, err
		}

		s.lock = &LockAttempt{
			version: index,
			request: request,
		}
		return &pb.LockResponse{
			Header:  header,
			Version: index,
		}, nil
	}
}

func (s *TestServer) Unlock(ctx context.Context, request *pb.UnlockRequest) (*pb.UnlockResponse, error) {
	s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.RequestId
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	if s.lock == nil || (request.Version != 0 && s.lock.version != request.Version) {
		return &pb.UnlockResponse{
			Header:   header,
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
		Header:   header,
		Unlocked: true,
	}, nil
}

func (s *TestServer) IsLocked(ctx context.Context, request *pb.IsLockedRequest) (*pb.IsLockedResponse, error) {
	session, err := s.GetSession(request.Header.SessionId)
	if err != nil {
		return nil, err
	}

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	if s.lock == nil {
		return &pb.IsLockedResponse{
			Header:   header,
			IsLocked: false,
		}, nil
	} else if request.Version > 0 && s.lock.version != request.Version {
		return &pb.IsLockedResponse{
			Header:   header,
			IsLocked: false,
		}, nil
	} else {
		return &pb.IsLockedResponse{
			Header:   header,
			IsLocked: true,
		}, nil
	}
}

func TestLock(t *testing.T) {
	conn, server := test.StartTestServer(func(server *grpc.Server) {
		pb.RegisterLockServiceServer(server, NewTestServer())
	})

	l1, err := newLock(context.TODO(), primitive.NewName("default", "test", "default", "test"), conn)
	assert.NoError(t, err)

	l2, err := newLock(context.TODO(), primitive.NewName("default", "test", "default", "test"), conn)
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

	locked, err = l1.IsLocked(context.Background(), WithIsVersion(v1))
	assert.NoError(t, err)
	assert.False(t, locked)

	locked, err = l1.IsLocked(context.Background(), WithIsVersion(v2))
	assert.NoError(t, err)
	assert.True(t, locked)

	test.StopTestServer(server)
}
