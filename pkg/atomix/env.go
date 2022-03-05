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

package atomix

import (
	"github.com/google/uuid"
	"os"
	"strconv"
	"sync"
)

const (
	clientIDEnv = "ATOMIX_CLIENT_ID"
	hostEnv     = "ATOMIX_BROKER_HOST"
	portEnv     = "ATOMIX_BROKER_PORT"
)

const defaultHost = "127.0.0.1"
const defaultPort = 5678

var envClient *Client
var envClientMu sync.RWMutex

func getClient() *Client {
	envClientMu.RLock()
	client := envClient
	envClientMu.RUnlock()
	if client != nil {
		return client
	}

	envClientMu.Lock()
	defer envClientMu.Unlock()

	clientID := os.Getenv(clientIDEnv)
	if clientID == "" {
		clientID = uuid.New().String()
	}

	host := os.Getenv(hostEnv)
	if host == "" {
		host = defaultHost
	}

	port := defaultPort
	ports := os.Getenv(portEnv)
	if ports != "" {
		i, err := strconv.Atoi(ports)
		if err != nil {
			panic(err)
		}
		port = i
	}

	client = NewClient(WithClientID(clientID), WithBrokerHost(host), WithBrokerPort(port))
	envClient = client
	return client
}
