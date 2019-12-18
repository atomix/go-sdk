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
	"fmt"
	netutil "github.com/atomix/atomix-go-client/pkg/client/util/net"
	"github.com/atomix/atomix-go-local/pkg/atomix/local"
	"github.com/atomix/atomix-go-node/pkg/atomix/registry"
	"net"
)

const basePort = 5000

// StartTestPartitions starts the given number of local partitions and returns client connections for them
func StartTestPartitions(partitions int) ([]netutil.Address, []chan struct{}) {
	addresses := make([]netutil.Address, partitions)
	chans := make([]chan struct{}, partitions)
	for i := 0; i < partitions; i++ {
		address, ch := startTestPartition()
		addresses[i] = address
		chans[i] = ch
	}
	return addresses, chans
}

// startTestPartition starts a single local partition
func startTestPartition() (netutil.Address, chan struct{}) {
	for port := basePort; port < basePort+100; port++ {
		address := netutil.Address(fmt.Sprintf("localhost:%d", port))
		lis, err := net.Listen("tcp", string(address))
		if err != nil {
			continue
		}
		node := local.NewNode(lis, registry.Registry)
		node.Start()

		ch := make(chan struct{})
		go func() {
			<-ch
			node.Stop()
		}()
		return address, ch
	}
	panic("cannot find open port")
}

// StopTestPartitions stops the given test partition channels
func StopTestPartitions(chans []chan struct{}) {
	for _, ch := range chans {
		close(ch)
	}
}
