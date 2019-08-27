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

package counter

import (
	"context"
	api "github.com/atomix/atomix-api/proto/atomix/counter"
	"github.com/atomix/atomix-api/proto/atomix/headers"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
)

func TestSession(t *testing.T) {
	client := &testCounterServiceClient{
		create: make(chan bool, 1),
		close:  make(chan bool, 1),
	}
	name := primitive.NewName("default", "test", "default", "test")
	handler := &sessionHandler{client}
	sess, err := session.New(context.TODO(), name, handler)
	assert.NoError(t, err)
	assert.True(t, client.created())
	assert.Equal(t, "default", sess.Name.Namespace)
	assert.Equal(t, "test", sess.Name.Name)
	assert.Equal(t, uint64(0), sess.SessionID)
	assert.Equal(t, uint64(10), sess.GetState().Index)
	err = sess.Close()
	assert.NoError(t, err)
	assert.True(t, client.closed())
}

type testCounterServiceClient struct {
	create chan bool
	close  chan bool
}

func (c *testCounterServiceClient) created() bool {
	return <-c.create
}

func (c *testCounterServiceClient) closed() bool {
	return <-c.close
}

func (c *testCounterServiceClient) Create(ctx context.Context, request *api.CreateRequest, opts ...grpc.CallOption) (*api.CreateResponse, error) {
	c.create <- true
	return &api.CreateResponse{
		Header: &headers.ResponseHeader{
			SessionID: uint64(0),
			Index:     uint64(10),
		},
	}, nil
}

func (c *testCounterServiceClient) Close(ctx context.Context, request *api.CloseRequest, opts ...grpc.CallOption) (*api.CloseResponse, error) {
	c.close <- true
	return &api.CloseResponse{
		Header: &headers.ResponseHeader{
			SessionID: request.Header.SessionID,
		},
	}, nil
}

func (c *testCounterServiceClient) CheckAndSet(ctx context.Context, in *api.CheckAndSetRequest, opts ...grpc.CallOption) (*api.CheckAndSetResponse, error) {
	return nil, nil
}

func (c *testCounterServiceClient) Decrement(ctx context.Context, in *api.DecrementRequest, opts ...grpc.CallOption) (*api.DecrementResponse, error) {
	return nil, nil
}

func (c *testCounterServiceClient) Get(ctx context.Context, in *api.GetRequest, opts ...grpc.CallOption) (*api.GetResponse, error) {
	return nil, nil
}

func (c *testCounterServiceClient) Increment(ctx context.Context, in *api.IncrementRequest, opts ...grpc.CallOption) (*api.IncrementResponse, error) {
	return nil, nil
}

func (c *testCounterServiceClient) Set(ctx context.Context, in *api.SetRequest, opts ...grpc.CallOption) (*api.SetResponse, error) {
	return nil, nil
}
