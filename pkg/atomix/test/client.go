// Copyright 2020-present Open Networking Foundation.
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

package test

import (
	"context"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

// Client is an interface for implementing the client for a test protocol
type Client interface {
	// Start starts the client
	Start(driverPort, agentPort int) error
	// Connect connects the client to the given primitive server
	Connect(ctx context.Context, primitive primitive.Type, name string) (*grpc.ClientConn, error)
	// Stop stops the client
	Stop() error
}

func newClient(id string, client Client) *testClient {
	return &testClient{
		Client: client,
		id:     id,
	}
}

type testClient struct {
	Client
	id string
}

func (c *testClient) getOpts(opts ...primitive.Option) []primitive.Option {
	return append([]primitive.Option{primitive.WithSessionID(c.id)}, opts...)
}

func (c *testClient) Close() error {
	return c.Client.Stop()
}
