// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package atomix

import (
	"context"
	"fmt"
	brokerapi "github.com/atomix/atomix-api/go/atomix/management/broker"
	primitiveapi "github.com/atomix/atomix-api/go/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util/retry"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"sync"
	"time"
)

// NewClient creates a new Atomix client
func NewClient(opts ...Option) *Client {
	options := clientOptions{
		clientID:   uuid.New().String(),
		brokerHost: defaultHost,
		brokerPort: defaultPort,
	}
	for _, opt := range opts {
		opt.apply(&options)
	}
	return &Client{
		options:        options,
		primitiveConns: make(map[primitiveapi.PrimitiveId]*grpc.ClientConn),
	}
}

type Client struct {
	options        clientOptions
	brokerConn     *grpc.ClientConn
	primitiveConns map[primitiveapi.PrimitiveId]*grpc.ClientConn
	mu             sync.RWMutex
}

func (c *Client) connect(ctx context.Context, primitive primitiveapi.PrimitiveId) (*grpc.ClientConn, error) {
	c.mu.RLock()
	driverConn, ok := c.primitiveConns[primitive]
	c.mu.RUnlock()
	if ok {
		return driverConn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	driverConn, ok = c.primitiveConns[primitive]
	if ok {
		return driverConn, nil
	}

	brokerConn := c.brokerConn
	if brokerConn == nil {
		conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", c.options.brokerHost, c.options.brokerPort),
			grpc.WithInsecure(),
			grpc.WithUnaryInterceptor(retry.RetryingUnaryClientInterceptor(retry.WithRetryOn(codes.Unavailable))))
		if err != nil {
			return nil, err
		}
		c.brokerConn = conn
		brokerConn = conn
	}

	brokerClient := brokerapi.NewBrokerClient(brokerConn)
	request := &brokerapi.LookupPrimitiveRequest{
		PrimitiveID: brokerapi.PrimitiveId{
			PrimitiveId: primitive,
		},
	}
	response, err := brokerClient.LookupPrimitive(ctx, request, retry.WithRetryOn(codes.Unavailable, codes.NotFound), retry.WithPerCallTimeout(time.Second))
	if err != nil {
		return nil, errors.From(err)
	}

	driverConn, err = grpc.DialContext(ctx, fmt.Sprintf("%s:%d", response.Address.Host, response.Address.Port),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(retry.RetryingUnaryClientInterceptor(retry.WithRetryOn(codes.Unavailable))),
		grpc.WithStreamInterceptor(retry.RetryingStreamClientInterceptor(retry.WithRetryOn(codes.Unavailable))))
	if err != nil {
		return nil, err
	}
	c.primitiveConns[primitive] = driverConn
	return driverConn, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.primitiveConns {
		conn.Close()
	}
	if c.brokerConn != nil {
		return c.brokerConn.Close()
	}
	return nil
}
