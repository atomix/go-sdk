// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"github.com/atomix/go-client/pkg/atomix/primitive"
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
