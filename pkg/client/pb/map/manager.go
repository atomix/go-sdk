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
	"context"
	"github.com/atomix/api/proto/atomix/pb/headers"
	mapapi "github.com/atomix/api/proto/atomix/pb/map"
	"github.com/atomix/go-client/pkg/client/pb/replica"
	"github.com/atomix/go-client/pkg/client/primitive"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
)

var manager *primaryBackupMapManager

var managers = make(map[replica.ID]*primaryBackupMapManager)

// getManager returns the primary-backup map manager
func getManager(replicaID replica.ID) *primaryBackupMapManager {
	manager, ok := managers[replicaID]
	if !ok {
		manager = &primaryBackupMapManager{
			servers: make(map[string]map[replica.GroupID]mapReplicaServer),
		}
		managers[replicaID] = manager
	}
	return manager
}

type mapReplicaServer interface {
	put(context.Context, *mapapi.PutRequest) (*mapapi.PutResponse, error)
	get(context.Context, *mapapi.GetRequest) (*mapapi.GetResponse, error)
	remove(context.Context, *mapapi.RemoveRequest) (*mapapi.RemoveResponse, error)
	backup(mapapi.MapService_BackupServer) error
}

// primaryBackupMapManager manages primary-backup map streams
type primaryBackupMapManager struct {
	servers map[string]map[replica.GroupID]mapReplicaServer
	mu      sync.RWMutex
}

func (s *primaryBackupMapManager) register(name primitive.Name, partition replica.GroupID, server mapReplicaServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	servers, ok := s.servers[name.String()]
	if !ok {
		servers = make(map[replica.GroupID]mapReplicaServer)
		s.servers[name.String()] = servers
	}
	servers[partition] = server
}

func (s *primaryBackupMapManager) getServer(header headers.RequestHeader) (mapReplicaServer, error) {
	name := primitive.Name{
		Namespace: header.Protocol.Namespace,
		Protocol:  header.Protocol.Name,
		Scope:     header.Primitive.Namespace,
		Name:      header.Primitive.Name,
	}
	s.mu.RLock()
	servers, ok := s.servers[name.String()]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "map not found")
	}
	partition := replica.GroupID(header.Partition)
	s.mu.RLock()
	server, ok := servers[partition]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "map not found")
	}
	return server, nil
}

func (s *primaryBackupMapManager) Put(ctx context.Context, request *mapapi.PutRequest) (*mapapi.PutResponse, error) {
	server, err := s.getServer(request.Header)
	if err != nil {
		return nil, err
	}
	return server.put(ctx, request)
}

func (s *primaryBackupMapManager) Get(ctx context.Context, request *mapapi.GetRequest) (*mapapi.GetResponse, error) {
	server, err := s.getServer(request.Header)
	if err != nil {
		return nil, err
	}
	return server.get(ctx, request)
}

func (s *primaryBackupMapManager) Remove(ctx context.Context, request *mapapi.RemoveRequest) (*mapapi.RemoveResponse, error) {
	server, err := s.getServer(request.Header)
	if err != nil {
		return nil, err
	}
	return server.remove(ctx, request)
}

func (s *primaryBackupMapManager) Backup(stream mapapi.MapService_BackupServer) error {
	for {
		request, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		server, err := s.getServer(request.Header)
		if err != nil {
			return err
		}
		err = server.backup(stream)
		if err != nil {
			return err
		}
	}
}
