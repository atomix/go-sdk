// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package atomix

import (
	"github.com/atomix/go-client/pkg/atomix/client"
	"sync"
)

var globalClient *client.Client
var globalClientMu sync.RWMutex

func getClient() *client.Client {
	globalClientMu.RLock()
	if globalClient != nil {
		defer globalClientMu.RUnlock()
		return globalClient
	}
	globalClientMu.RUnlock()

	globalClientMu.Lock()
	defer globalClientMu.Unlock()
	if globalClient == nil {
		globalClient = client.NewClient()
	}
	return globalClient
}
