// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
