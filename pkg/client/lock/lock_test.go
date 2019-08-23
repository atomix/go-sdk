// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lock

import (
	"context"
	api "github.com/atomix/atomix-api/proto/atomix/lock"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/test"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"time"
)

// NewTestServer creates a new server for managing sessions
func NewTestServer() *TestServer {
	return &TestServer{
		Server: test.NewServer(),
		queue:  []*LockAttempt{},
	}
}

type TestServer struct {
	*test.Server
	lock  *LockAttempt
	queue []*LockAttempt
}

type LockAttempt struct {
	version uint64
	request *api.LockRequest
	time    time.Time
	c       chan<- bool
}

func (s *TestServer) Create(ctx context.Context, request *api.CreateRequest) (*api.CreateResponse, error) {
	header, err := s.CreateHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &api.CreateResponse{
		Header: header,
	}, nil
}

func (s *TestServer) KeepAlive(ctx context.Context, request *api.KeepAliveRequest) (*api.KeepAliveResponse, error) {
	header, err := s.KeepAliveHeader(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	return &api.KeepAliveResponse{
		Header: header,
	}, nil
}

func (s *TestServer) Close(ctx context.Context, request *api.CloseRequest) (*api.CloseResponse, error) {
	err := s.CloseHeader(ctx, request.Header)
	if err != nil {
		return nil, err
	}
	return &api.CloseResponse{}, nil
}

func (s *TestServer) Lock(ctx context.Context, request *api.LockRequest) (*api.LockResponse, error) {
	index := s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.RequestID
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
			return &api.LockResponse{
				Header:  header,
				Version: s.Index,
			}, nil
		}
		return &api.LockResponse{
			Header:  header,
			Version: 0,
		}, nil
	}

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	s.lock = &LockAttempt{
		version: index,
		request: request,
	}
	return &api.LockResponse{
		Header:  header,
		Version: index,
	}, nil
}

func (s *TestServer) Unlock(ctx context.Context, request *api.UnlockRequest) (*api.UnlockResponse, error) {
	s.IncrementIndex()

	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return nil, err
	}

	sequenceNumber := request.Header.RequestID
	session.Await(sequenceNumber)
	defer session.Complete(sequenceNumber)

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	if s.lock == nil || (request.Version != 0 && s.lock.version != request.Version) {
		return &api.UnlockResponse{
			Header:   header,
			Unlocked: false,
		}, nil
	}

	s.lock = nil

	if len(s.queue) > 0 {
		attempt := s.queue[0]
		s.queue = s.queue[1:]

		if attempt.request.Timeout != nil {
			t := *attempt.request.Timeout
			d := time.Since(attempt.time)
			if int64(d) > int64(t) {
				attempt.c <- false
			} else {
				attempt.c <- true
			}
		} else {
			attempt.c <- true
		}
	}

	return &api.UnlockResponse{
		Header:   header,
		Unlocked: true,
	}, nil
}

func (s *TestServer) IsLocked(ctx context.Context, request *api.IsLockedRequest) (*api.IsLockedResponse, error) {
	session, err := s.GetSession(request.Header.SessionID)
	if err != nil {
		return nil, err
	}

	header, err := session.NewResponseHeader()
	if err != nil {
		return nil, err
	}

	if s.lock == nil {
		return &api.IsLockedResponse{
			Header:   header,
			IsLocked: false,
		}, nil
	} else if request.Version > 0 && s.lock.version != request.Version {
		return &api.IsLockedResponse{
			Header:   header,
			IsLocked: false,
		}, nil
	} else {
		return &api.IsLockedResponse{
			Header:   header,
			IsLocked: true,
		}, nil
	}
}

func TestLock(t *testing.T) {
	conn, server := test.StartTestServer(func(server *grpc.Server) {
		api.RegisterLockServiceServer(server, NewTestServer())
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
