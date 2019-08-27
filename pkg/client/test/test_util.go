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

package test

import (
	"context"
	"github.com/atomix/atomix-go-local/pkg/atomix/local"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"net"
)

// StartTestPartitions starts the given number of local partitions and returns client connections for them
func StartTestPartitions(partitions int) ([]*grpc.ClientConn, []chan struct{}) {
	conns := make([]*grpc.ClientConn, partitions)
	chans := make([]chan struct{}, partitions)
	for i := 0; i < partitions; i++ {
		conn, ch := startTestPartition()
		conns[i] = conn
		chans[i] = ch
	}
	return conns, chans
}

// startTestPartition starts a single local partition
func startTestPartition() (*grpc.ClientConn, chan struct{}) {
	lis := bufconn.Listen(1024 * 1024)
	node := local.NewNode(lis)
	go func() {
		_ = node.Start()
	}()

	dialer := func(ctx context.Context, address string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.DialContext(context.Background(), "devices", grpc.WithContextDialer(dialer), grpc.WithInsecure())
	if err != nil {
		panic("Failed to dial devices")
	}

	ch := make(chan struct{})
	go func() {
		<-ch
		_ = node.Stop()
	}()
	return conn, ch
}

// StopTestPartitions stops the given test partition channels
func StopTestPartitions(chans []chan struct{}) {
	for _, ch := range chans {
		close(ch)
	}
}
