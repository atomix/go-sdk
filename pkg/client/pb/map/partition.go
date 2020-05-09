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
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/atomix/api/proto/atomix/pb/headers"
	mapapi "github.com/atomix/api/proto/atomix/pb/map"
	primitiveapi "github.com/atomix/api/proto/atomix/primitive"
	"github.com/atomix/api/proto/atomix/protocol"
	"github.com/atomix/go-client/pkg/client/pb/replica"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"time"
)

func newPartition(ctx context.Context, name primitive.Name, replicas *replica.Group, opts ...Option) (Map, error) {
	options := applyOptions(opts...)
	partition := &mapPartition{
		name:     name,
		options:  options,
		group:    replicas,
		entries:  make(map[string]mapapi.Entry),
		backups:  make(map[replica.ID]*mapBackup),
		backupCh: make(chan mapapi.Entry),
		eventCh:  make(chan Event),
		watchers: make(map[string]chan<- Event),
	}
	if err := partition.open(ctx); err != nil {
		return nil, err
	}
	return partition, nil
}

// mapPartition is a map partition
type mapPartition struct {
	name       primitive.Name
	options    options
	group      *replica.Group
	entries    map[string]mapapi.Entry
	entriesMu  sync.RWMutex
	replicas   replica.Set
	backups    map[replica.ID]*mapBackup
	backupCh   chan mapapi.Entry
	eventCh    chan Event
	watchers   map[string]chan<- Event
	watchersMu sync.RWMutex
	timestamp  uint64
	replicasMu sync.RWMutex
	closeCh    chan struct{}
}

func (m *mapPartition) Name() primitive.Name {
	return m.name
}

func (m *mapPartition) open(ctx context.Context) error {
	local := m.group.Local()
	if local != nil {
		getManager(local.ID).register(m.name, m.group.ID, m)
	}
	ch := make(chan replica.Set)
	err := m.group.Watch(context.Background(), ch)
	if err != nil {
		return err
	}
	go m.processReplicaChanges(ch)
	go m.processUpdates()
	go m.processEvents()
	return nil
}

func (m *mapPartition) processReplicaChanges(ch chan replica.Set) {
	for {
		select {
		case replicas := <-ch:
			m.replicasMu.Lock()
			if m.group.Local() != nil && replicas.Primary != nil &&
				replicas.Primary.ID == m.group.Local().ID &&
				(m.replicas.Primary == nil || m.replicas.Primary.ID != m.group.Local().ID) {
				m.timestamp = 0
				backups := make(map[replica.ID]*mapBackup)
				for _, replica := range replicas.Backups {
					backup, ok := m.backups[replica.ID]
					if !ok {
						backup = newBackup(m, &replica)
						err := backup.start()
						if err != nil {
							fmt.Println(err)
						} else {
							backups[replica.ID] = backup
						}
					} else {
						backups[replica.ID] = backup
					}
				}
				for id, backup := range m.backups {
					if _, ok := backups[id]; !ok {
						backup.stop()
					}
				}
				m.backups = backups
			}
			m.replicas = replicas
			m.replicasMu.Unlock()
		case <-m.closeCh:
			return
		}
	}
}

func (m *mapPartition) processUpdates() {
	for {
		select {
		case entry := <-m.backupCh:
			m.replicasMu.RLock()
			for _, backup := range m.backups {
				backup.backup(entry)
			}
			m.replicasMu.RUnlock()
		case <-m.closeCh:
			return
		}
	}
}

func (m *mapPartition) processEvents() {
	for {
		select {
		case event := <-m.eventCh:
			m.publishEvent(event)
		case <-m.closeCh:
			return
		}
	}
}

func (m *mapPartition) publishEvent(event Event) {
	m.watchersMu.RLock()
	for _, watcher := range m.watchers {
		watcher <- event
	}
	m.watchersMu.RUnlock()
}

func (m *mapPartition) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry, error) {
	m.replicasMu.RLock()
	replicas := m.replicas
	m.replicasMu.RUnlock()

	options := applyPutOptions(opts...)

	local := m.group.Local()
	if local != nil && replicas.Primary != nil && replicas.Primary.ID == local.ID {
		m.entriesMu.Lock()
		defer m.entriesMu.Unlock()
		version := m.timestamp + 1
		m.timestamp = version
		entry := mapapi.Entry{
			Key: key,
			Value: &mapapi.Value{
				Value:   value,
				Version: uint64(version),
			},
			Digest: mapapi.Digest{
				Term:      uint64(replicas.Term),
				Timestamp: version,
			},
		}
		m.entries[key] = entry
		m.backupCh <- entry
		return &Entry{
			Key:     key,
			Value:   value,
			Version: Version(version),
		}, nil
	}

	if replicas.Primary != nil {
		conn, err := replicas.Primary.Connect()
		if err != nil {
			return nil, err
		}
		client := mapapi.NewMapServiceClient(conn)
		request := &mapapi.PutRequest{
			Header: headers.RequestHeader{
				Protocol: protocol.ProtocolId{
					Namespace: m.name.Namespace,
					Name:      m.name.Protocol,
				},
				Primitive: primitiveapi.PrimitiveId{
					Namespace: m.name.Scope,
					Name:      m.name.Name,
				},
				Partition: uint32(m.group.ID),
			},
			Key: key,
			Value: mapapi.Value{
				Value:   value,
				Version: uint64(options.version),
			},
		}
		response, err := client.Put(ctx, request)
		if err != nil {
			return nil, err
		}
		return &Entry{
			Key:     key,
			Value:   value,
			Version: Version(response.Version),
		}, nil
	}
	return nil, errors.New("no primary")
}

func (m *mapPartition) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
	m.replicasMu.RLock()
	replicas := m.replicas
	m.replicasMu.RUnlock()

	local := m.group.Local()
	if local != nil && replicas.Primary != nil && replicas.Primary.ID == local.ID {
		m.entriesMu.RLock()
		defer m.entriesMu.RUnlock()
		entry, ok := m.entries[key]
		if !ok {
			return nil, nil
		}
		return &Entry{
			Key:     key,
			Value:   entry.Value.Value,
			Version: Version(entry.Value.Version),
		}, nil
	}

	if replicas.Primary != nil {
		conn, err := replicas.Primary.Connect()
		if err != nil {
			return nil, err
		}
		client := mapapi.NewMapServiceClient(conn)
		request := &mapapi.GetRequest{
			Header: headers.RequestHeader{
				Protocol: protocol.ProtocolId{
					Namespace: m.name.Namespace,
					Name:      m.name.Protocol,
				},
				Primitive: primitiveapi.PrimitiveId{
					Namespace: m.name.Scope,
					Name:      m.name.Name,
				},
				Partition: uint32(m.group.ID),
			},
			Key: key,
		}
		response, err := client.Get(ctx, request)
		if err != nil {
			return nil, err
		}
		if response.Value == nil {
			return nil, nil
		}
		return &Entry{
			Key:     key,
			Value:   response.Value.Value,
			Version: Version(response.Value.Version),
		}, nil
	}
	return nil, errors.New("no primary")
}

func (m *mapPartition) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error) {
	m.replicasMu.RLock()
	replicas := m.replicas
	m.replicasMu.RUnlock()

	options := applyRemoveOptions(opts...)

	local := m.group.Local()
	if local != nil && replicas.Primary != nil && replicas.Primary.ID == local.ID {
		m.entriesMu.Lock()
		defer m.entriesMu.Unlock()
		version := m.timestamp + 1
		m.timestamp = version
		entry, ok := m.entries[key]
		if !ok {
			if options.version > 0 {
				return nil, errors.New("version mismatch")
			}
			return nil, nil
		}
		if options.version > 0 && Version(entry.Value.Version) != options.version {
			return nil, errors.New("version mismatch")
		}
		delete(m.entries, key)
		update := mapapi.Entry{
			Key: key,
			Digest: mapapi.Digest{
				Term:      uint64(replicas.Term),
				Timestamp: version,
			},
		}
		m.backupCh <- update
		return &Entry{
			Key:     key,
			Value:   entry.Value.Value,
			Version: Version(entry.Value.Version),
		}, nil
	}

	if replicas.Primary != nil {
		conn, err := replicas.Primary.Connect()
		if err != nil {
			return nil, err
		}
		client := mapapi.NewMapServiceClient(conn)
		request := &mapapi.RemoveRequest{
			Header: headers.RequestHeader{
				Protocol: protocol.ProtocolId{
					Namespace: m.name.Namespace,
					Name:      m.name.Protocol,
				},
				Primitive: primitiveapi.PrimitiveId{
					Namespace: m.name.Scope,
					Name:      m.name.Name,
				},
				Partition: uint32(m.group.ID),
			},
			Key: key,
			Value: mapapi.Value{
				Version: uint64(options.version),
			},
		}
		response, err := client.Remove(ctx, request)
		if err != nil {
			return nil, err
		}
		if response.Value == nil {
			return nil, nil
		}
		return &Entry{
			Key:     key,
			Value:   response.Value.Value,
			Version: Version(response.Value.Version),
		}, nil
	}
	return nil, errors.New("no primary")
}

func (m *mapPartition) Len(ctx context.Context) (int, error) {
	panic("implement me")
}

func (m *mapPartition) Clear(ctx context.Context) error {
	panic("implement me")
}

func (m *mapPartition) Entries(ctx context.Context, ch chan<- Entry) error {
	panic("implement me")
}

func (m *mapPartition) Watch(ctx context.Context, ch chan<- Event, opts ...WatchOption) error {
	id := uuid.New().String()
	m.watchersMu.Lock()
	m.watchers[id] = ch
	m.watchersMu.Unlock()
	go func() {
		<-ctx.Done()
		m.watchersMu.Lock()
		delete(m.watchers, id)
		m.watchersMu.Unlock()
	}()
	return nil
}

func (m *mapPartition) Close(ctx context.Context) error {
	close(m.closeCh)
	return nil
}

func (m *mapPartition) Delete(ctx context.Context) error {
	return nil
}

func (m *mapPartition) put(ctx context.Context, request *mapapi.PutRequest) (*mapapi.PutResponse, error) {
	m.replicasMu.RLock()
	replicas := m.replicas
	m.replicasMu.RUnlock()
	if m.group.Local() == nil || replicas.Primary == nil || replicas.Primary.ID != m.group.Local().ID {
		return nil, status.Error(codes.Unavailable, "not the primary")
	}

	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()

	if request.Value.Version > 0 {
		entry, ok := m.entries[request.Key]
		if !ok || entry.Value.Version != request.Value.Version {
			return nil, status.Error(codes.PermissionDenied, "optimistic lock failure")
		}
	}

	timestamp := m.timestamp + 1
	m.timestamp = timestamp
	entry := mapapi.Entry{
		Key: request.Key,
		Value: &mapapi.Value{
			Value:   request.Value.Value,
			Version: timestamp,
		},
		Digest: mapapi.Digest{
			Term:      uint64(replicas.Term),
			Timestamp: timestamp,
		},
	}
	m.entries[request.Key] = entry
	m.backupCh <- entry
	return &mapapi.PutResponse{
		Header: headers.ResponseHeader{
			Type:   headers.ResponseType_RESPONSE,
			Status: headers.ResponseStatus_OK,
		},
		Version: timestamp,
	}, nil
}

func (m *mapPartition) get(ctx context.Context, request *mapapi.GetRequest) (*mapapi.GetResponse, error) {
	m.replicasMu.RLock()
	replicas := m.replicas
	m.replicasMu.RUnlock()
	if m.group.Local() == nil || replicas.Primary == nil || replicas.Primary.ID != m.group.Local().ID {
		return nil, status.Error(codes.Unavailable, "not the primary")
	}

	m.entriesMu.RLock()
	defer m.entriesMu.RUnlock()

	entry, ok := m.entries[request.Key]
	if !ok {
		return &mapapi.GetResponse{
			Header: headers.ResponseHeader{
				Type:   headers.ResponseType_RESPONSE,
				Status: headers.ResponseStatus_OK,
			},
		}, nil
	}
	return &mapapi.GetResponse{
		Header: headers.ResponseHeader{
			Type:   headers.ResponseType_RESPONSE,
			Status: headers.ResponseStatus_OK,
		},
		Value: &mapapi.Value{
			Value:   entry.Value.Value,
			Version: entry.Value.Version,
		},
	}, nil
}

func (m *mapPartition) remove(ctx context.Context, request *mapapi.RemoveRequest) (*mapapi.RemoveResponse, error) {
	m.replicasMu.RLock()
	replicas := m.replicas
	m.replicasMu.RUnlock()
	if m.group.Local() == nil || replicas.Primary == nil || replicas.Primary.ID != m.group.Local().ID {
		return nil, status.Error(codes.Unavailable, "not the primary")
	}

	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()

	entry, ok := m.entries[request.Key]
	if request.Value.Version > 0 && (!ok || entry.Value.Version != request.Value.Version) {
		return nil, status.Error(codes.PermissionDenied, "optimistic lock failure")
	}

	if !ok {
		return &mapapi.RemoveResponse{
			Header: headers.ResponseHeader{
				Type:   headers.ResponseType_RESPONSE,
				Status: headers.ResponseStatus_OK,
			},
		}, nil
	}

	delete(m.entries, request.Key)

	timestamp := m.timestamp + 1
	m.timestamp = timestamp
	update := mapapi.Entry{
		Key: request.Key,
		Digest: mapapi.Digest{
			Term:      uint64(replicas.Term),
			Timestamp: timestamp,
		},
	}
	m.backupCh <- update
	return &mapapi.RemoveResponse{
		Header: headers.ResponseHeader{
			Type:   headers.ResponseType_RESPONSE,
			Status: headers.ResponseStatus_OK,
		},
		Value: &mapapi.Value{
			Value:   entry.Value.Value,
			Version: entry.Value.Version,
		},
	}, nil
}

func (m *mapPartition) backup(stream mapapi.MapService_BackupServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		for _, entry := range request.Entries {
			m.update(entry)
		}
	}
}

func (m *mapPartition) update(update mapapi.Entry) {
	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()
	current, ok := m.entries[update.Key]
	if update.Value != nil {
		if !ok || update.Digest.Term > current.Digest.Term || (update.Digest.Term == current.Digest.Term && update.Digest.Timestamp > current.Digest.Timestamp) {
			m.entries[update.Key] = update
		}
	} else {
		if ok && (update.Digest.Term > current.Digest.Term || (update.Digest.Term == current.Digest.Term && update.Digest.Timestamp > current.Digest.Timestamp)) {
			delete(m.entries, update.Key)
		}
	}
}

func newBackup(partition *mapPartition, replica *replica.Replica) *mapBackup {
	return &mapBackup{
		partition: partition,
		replica:   replica,
		queue:     list.New(),
		backupCh:  make(chan struct{}),
		closeCh:   make(chan struct{}),
	}
}

type mapBackup struct {
	partition *mapPartition
	replica   *replica.Replica
	queue     *list.List
	backupCh  chan struct{}
	closeCh   chan struct{}
	mu        sync.RWMutex
}

func (b *mapBackup) backup(entry mapapi.Entry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queue.PushBack(entry)
	if b.queue.Len() > b.partition.options.maxBackupQueue {
		b.backupCh <- struct{}{}
	}
}

func (b *mapBackup) start() error {
	conn, err := b.replica.Connect()
	if err != nil {
		return err
	}
	client := mapapi.NewMapServiceClient(conn)
	stream, err := client.Backup(context.Background())
	if err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(b.partition.options.backupPeriod)
		timedOut := false
		for {
			select {
			case <-ticker.C:
				if timedOut {
					b.mu.Lock()
					if b.queue.Len() > 0 {
						entries := make([]mapapi.Entry, 0, b.queue.Len())
						entry := b.queue.Front()
						for entry != nil {
							entries = append(entries, entry.Value.(mapapi.Entry))
							next := entry.Next()
							b.queue.Remove(entry)
							entry = next
						}
						b.mu.Unlock()
						err := stream.Send(&mapapi.BackupRequest{
							Header: headers.RequestHeader{
								Protocol: protocol.ProtocolId{
									Namespace: b.partition.name.Namespace,
									Name:      b.partition.name.Protocol,
								},
								Primitive: primitiveapi.PrimitiveId{
									Namespace: b.partition.name.Scope,
									Name:      b.partition.name.Name,
								},
								Partition: uint32(b.partition.group.ID),
							},
						})
						if err != nil {
							fmt.Println(err)
						}
					} else {
						b.mu.Unlock()
					}
				} else {
					timedOut = true
				}
			case <-b.backupCh:
				b.mu.Lock()
				if b.queue.Len() > 0 {
					entries := make([]mapapi.Entry, 0, b.queue.Len())
					entry := b.queue.Front()
					for entry != nil {
						entries = append(entries, entry.Value.(mapapi.Entry))
						next := entry.Next()
						b.queue.Remove(entry)
						entry = next
					}
					b.mu.Unlock()
					err := stream.Send(&mapapi.BackupRequest{
						Header: headers.RequestHeader{
							Protocol: protocol.ProtocolId{
								Namespace: b.partition.name.Namespace,
								Name:      b.partition.name.Protocol,
							},
							Primitive: primitiveapi.PrimitiveId{
								Namespace: b.partition.name.Scope,
								Name:      b.partition.name.Name,
							},
							Partition: uint32(b.partition.group.ID),
						},
					})
					if err != nil {
						fmt.Println(err)
					}
				} else {
					b.mu.Unlock()
				}
				timedOut = false
			}
		}
	}()
	return nil
}

func (b *mapBackup) stop() {
	close(b.closeCh)
}
