// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/utils/grpc/interceptors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

func NewClient(opts ...Option) *Client {
	var options Options
	options.apply(opts...)
	return &Client{
		Options: options,
	}
}

// Client is an Atomix runtime client
type Client struct {
	Options
	conn *grpc.ClientConn
	mu   sync.RWMutex
}

func (c *Client) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn, nil
	}

	target := fmt.Sprintf("%s:%d", c.Host, c.Port)
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(
			interceptors.ErrorHandlingUnaryClientInterceptor(),
			interceptors.RetryingUnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(
			interceptors.ErrorHandlingStreamClientInterceptor(),
			interceptors.RetryingStreamClientInterceptor()))
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer func() {
		c.conn = nil
	}()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
