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

package cluster

import (
	"fmt"
	"google.golang.org/grpc"
	"net"
	"sync"
)

// MemberID is a member identifier
type MemberID string

// NewMember returns a new cluster member
func NewMember(id MemberID, host string, port int) *Member {
	return &Member{
		ID:   id,
		Host: host,
		Port: port,
	}
}

// Member is a membership group member
type Member struct {
	ID   MemberID
	Host string
	Port int
	conn *grpc.ClientConn
	mu   sync.RWMutex
}

// Connect connects to the member
func (m *Member) Connect() (*grpc.ClientConn, error) {
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn != nil {
		return m.conn, nil
	}

	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", m.Host, m.Port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	m.conn = conn
	return conn, err
}

// NewLocalMember returns a new local cluster member
func NewLocalMember(id MemberID, host string, port int) *LocalMember {
	return &LocalMember{
		Member: &Member{
			ID:   id,
			Host: host,
			Port: port,
		},
	}
}

// LocalMember is a local cluster member
type LocalMember struct {
	*Member
	stopCh chan struct{}
}

// Serve begins serving the local member
func (m *LocalMember) Serve() error {
	server := grpc.NewServer()
	for _, service := range registry.services {
		service(m.ID, server)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", m.Port))
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
		<-m.stopCh
		server.Stop()
	}()
	return nil
}

// Stop stops the local member serving
func (m *LocalMember) Stop() {
	close(m.stopCh)
}
