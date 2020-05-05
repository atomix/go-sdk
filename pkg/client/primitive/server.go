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

package primitive

import (
	"fmt"
	"google.golang.org/grpc"
	"net"
)

// Serve starts the primitive server
func Serve(port int, done <-chan struct{}) error {
	server := grpc.NewServer()
	for _, service := range registry.services {
		service(server)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	go func() {
		err := server.Serve(lis)
		if err != nil {
			fmt.Println(err)
		}
	}()
	go func() {
		<-done
		server.Stop()
	}()
	return nil
}
