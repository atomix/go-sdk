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

package leader

import (
	"context"
	"github.com/atomix/atomix-api/proto/atomix/headers"
	api "github.com/atomix/atomix-api/proto/atomix/leader"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestSession(t *testing.T) {
	client := &testElectionServiceClient{
		create:    make(chan bool, 1),
		keepAlive: make(chan bool),
		close:     make(chan bool, 1),
	}
	name := primitive.NewName("default", "test", "default", "test")
	handler := &sessionHandler{client}
	sess, err := session.New(context.TODO(), name, handler, session.WithTimeout(2*time.Second))
	assert.NoError(t, err)
	assert.True(t, client.created())
	assert.Equal(t, "default", sess.Name.Namespace)
	assert.Equal(t, "test", sess.Name.Name)
	assert.Equal(t, uint64(1), sess.SessionID)
	assert.Equal(t, uint64(10), sess.GetState().Index)
	assert.True(t, client.keptAlive())
	err = sess.Close()
	assert.NoError(t, err)
	assert.True(t, client.closed())
}

type testElectionServiceClient struct {
	create    chan bool
	keepAlive chan bool
	close     chan bool
}

func (c *testElectionServiceClient) created() bool {
	return <-c.create
}

func (c *testElectionServiceClient) keptAlive() bool {
	return <-c.keepAlive
}

func (c *testElectionServiceClient) closed() bool {
	return <-c.close
}

func (c *testElectionServiceClient) Create(ctx context.Context, request *api.CreateRequest, opts ...grpc.CallOption) (*api.CreateResponse, error) {
	c.create <- true
	return &api.CreateResponse{
		Header: &headers.ResponseHeader{
			SessionID: uint64(1),
			Index:     uint64(10),
		},
	}, nil
}

func (c *testElectionServiceClient) KeepAlive(ctx context.Context, request *api.KeepAliveRequest, opts ...grpc.CallOption) (*api.KeepAliveResponse, error) {
	c.keepAlive <- true
	return &api.KeepAliveResponse{
		Header: &headers.ResponseHeader{
			SessionID: request.Header.SessionID,
		},
	}, nil
}

func (c *testElectionServiceClient) Close(ctx context.Context, request *api.CloseRequest, opts ...grpc.CallOption) (*api.CloseResponse, error) {
	c.close <- true
	return &api.CloseResponse{
		Header: &headers.ResponseHeader{
			SessionID: request.Header.SessionID,
		},
	}, nil
}

func (c *testElectionServiceClient) Latch(ctx context.Context, request *api.LatchRequest, opts ...grpc.CallOption) (*api.LatchResponse, error) {
	panic("implement me")
}

func (c *testElectionServiceClient) Get(ctx context.Context, request *api.GetRequest, opts ...grpc.CallOption) (*api.GetResponse, error) {
	panic("implement me")
}

func (c *testElectionServiceClient) Events(ctx context.Context, request *api.EventRequest, opts ...grpc.CallOption) (api.LeaderLatchService_EventsClient, error) {
	panic("implement me")
}
