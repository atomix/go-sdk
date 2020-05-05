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

package _map

import (
	mapapi "github.com/atomix/api/proto/atomix/gossip/map"
	"github.com/atomix/go-client/pkg/client/primitive"
	"io"
	"sync"
)

var manager *gossipMapManager

// getManager returns the gossip map manager
func getManager() *gossipMapManager {
	if manager == nil {
		manager = &gossipMapManager{
			instances: make(map[string]map[string]gossipMapHandler),
		}
	}
	return manager
}

// gossipMapHandler handles gossip map streams
type gossipMapHandler func(*mapapi.Message, mapapi.GossipMapService_ConnectServer) error

// gossipMapManager manages gossip map streams
type gossipMapManager struct {
	instances map[string]map[string]gossipMapHandler
	mu        sync.RWMutex
}

func (s *gossipMapManager) register(name primitive.Name, handler gossipMapHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	instances, ok := s.instances[name.Protocol]
	if !ok {
		instances = make(map[string]gossipMapHandler)
		s.instances[name.Protocol] = instances
	}
	instances[name.Name] = handler
}

func (s *gossipMapManager) Connect(stream mapapi.GossipMapService_ConnectServer) error {
	for {
		request, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		s.mu.RLock()
		instances, ok := s.instances[request.Header.Namespace]
		if ok {
			handler, ok := instances[request.Target.Name]
			s.mu.RUnlock()
			if ok {
				err := handler(request, stream)
				if err != nil {
					return err
				}
			}
		} else {
			s.mu.RUnlock()
		}
	}
}
