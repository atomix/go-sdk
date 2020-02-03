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

package session

import (
	"context"
	"github.com/atomix/api/proto/atomix/headers"
	"github.com/atomix/go-client/pkg/client/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSessionOptions(t *testing.T) {
	options := &options{}
	WithTimeout(5 * time.Second).prepare(options)
	assert.Equal(t, 5*time.Second, options.timeout)
}

func newTestHandler() *testHandler {
	return &testHandler{
		create:    make(chan bool, 1),
		keepAlive: make(chan bool),
		close:     make(chan bool, 1),
		delete:    make(chan bool, 1),
	}
}

type testHandler struct {
	create    chan bool
	keepAlive chan bool
	close     chan bool
	delete    chan bool
}

func (h *testHandler) Create(ctx context.Context, session *Session) error {
	h.create <- true
	return nil
}

func (h *testHandler) KeepAlive(ctx context.Context, session *Session) error {
	h.keepAlive <- true
	return nil
}

func (h *testHandler) Close(ctx context.Context, session *Session) error {
	h.close <- true
	return nil
}

func (h *testHandler) Delete(ctx context.Context, session *Session) error {
	h.delete <- true
	return nil
}

func TestSession(t *testing.T) {
	name := primitive.NewName("a", "b", "c", "d")
	handler := newTestHandler()
	session, err := New(context.TODO(), name, primitive.Partition{ID: 1, Address: "localhost:5000"}, handler, WithTimeout(5*time.Second))
	assert.NoError(t, err)
	assert.NotNil(t, session)
	assert.Equal(t, "c", session.Name.Namespace)
	assert.Equal(t, "d", session.Name.Name)
	assert.Equal(t, 5*time.Second, session.Timeout)
	assert.True(t, <-handler.create)

	header := session.getQueryHeader()
	assert.Equal(t, "c", header.Name.Namespace)
	assert.Equal(t, "d", header.Name.Name)
	assert.Equal(t, uint64(0), header.Index)
	assert.Equal(t, uint64(0), header.SessionID)

	session.RecordResponse(header, &headers.ResponseHeader{
		SessionID: uint64(1),
		Index:     uint64(10),
	})

	header = session.getQueryHeader()
	assert.Equal(t, "c", header.Name.Namespace)
	assert.Equal(t, "d", header.Name.Name)
	assert.Equal(t, uint64(10), header.Index)
	assert.Equal(t, uint64(1), header.SessionID)
	assert.Equal(t, uint64(0), header.RequestID)

	header = session.nextCommandHeader()
	assert.Equal(t, "c", header.Name.Namespace)
	assert.Equal(t, "d", header.Name.Name)
	assert.Equal(t, uint64(10), header.Index)
	assert.Equal(t, uint64(1), header.SessionID)
	assert.Equal(t, uint64(1), header.RequestID)

	session.RecordResponse(header, &headers.ResponseHeader{
		SessionID:  uint64(1),
		Index:      uint64(11),
		ResponseID: uint64(1),
		StreamID:   uint64(1),
	})

	stream, header := session.nextStreamHeader()
	assert.True(t, stream.Serialize(&headers.ResponseHeader{
		SessionID:  uint64(1),
		Index:      uint64(11),
		ResponseID: uint64(1),
		StreamID:   uint64(1),
	}))
	assert.True(t, stream.Serialize(&headers.ResponseHeader{
		SessionID:  uint64(1),
		Index:      uint64(11),
		ResponseID: uint64(2),
		StreamID:   uint64(1),
	}))
	assert.True(t, stream.Serialize(&headers.ResponseHeader{
		SessionID:  uint64(1),
		Index:      uint64(11),
		ResponseID: uint64(3),
		StreamID:   uint64(1),
	}))
	assert.False(t, stream.Serialize(&headers.ResponseHeader{
		SessionID:  uint64(1),
		Index:      uint64(11),
		ResponseID: uint64(5),
		StreamID:   uint64(1),
	}))
	assert.True(t, stream.Serialize(&headers.ResponseHeader{
		SessionID:  uint64(1),
		Index:      uint64(11),
		ResponseID: uint64(4),
		StreamID:   uint64(1),
	}))

	session.RecordResponse(header, &headers.ResponseHeader{
		SessionID:  uint64(1),
		Index:      uint64(12),
		ResponseID: stream.ID,
	})
	header = session.getState()
	assert.Equal(t, "c", header.Name.Namespace)
	assert.Equal(t, "d", header.Name.Name)
	assert.Equal(t, uint64(12), header.Index)
	assert.Equal(t, uint64(1), header.SessionID)
	assert.Equal(t, uint64(2), header.RequestID)
	assert.Len(t, header.Streams, 1)
	assert.Equal(t, uint64(4), header.Streams[0].ResponseID)

	assert.True(t, <-handler.keepAlive)
}
