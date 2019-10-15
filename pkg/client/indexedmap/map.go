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

package indexedmap

import (
	"context"
	"errors"
	"fmt"
	api "github.com/atomix/atomix-api/proto/atomix/indexedmap"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
	"time"
)

// Type is the indexedmap type
const Type primitive.Type = "indexedmap"

// Index is the index of an entry
type Index uint64

// Version is the version of an entry
type Version uint64

// Client provides an API for creating IndexedMaps
type Client interface {
	// GetIndexedMap gets the IndexedMap instance of the given name
	GetIndexedMap(ctx context.Context, name string, opts ...session.Option) (IndexedMap, error)
}

// IndexedMap is a distributed linked map
type IndexedMap interface {
	primitive.Primitive

	// Put sets a key/value pair in the map
	Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry, error)

	// Get gets the value of the given key
	Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error)

	// GetIndex gets the entry at the given index
	GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry, error)

	// Replace replaces the given key with the given value
	Replace(ctx context.Context, key string, value []byte, opts ...ReplaceOption) (*Entry, error)

	// ReplaceIndex replaces the given index with the given value
	ReplaceIndex(ctx context.Context, index Index, value []byte, opts ...ReplaceOption) (*Entry, error)

	// Remove removes a key from the map
	Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error)

	// RemoveIndex removes an index from the map
	RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry, error)

	// Len returns the number of entries in the map
	Len(ctx context.Context) (int, error)

	// Clear removes all entries from the map
	Clear(ctx context.Context) error

	// Entries lists the entries in the map
	// This is a non-blocking method. If the method returns without error, key/value paids will be pushed on to the
	// given channel and the channel will be closed once all entries have been read from the map.
	Entries(ctx context.Context, ch chan<- *Entry) error

	// Watch watches the map for changes
	// This is a non-blocking method. If the method returns without error, map events will be pushed onto
	// the given channel in the order in which they occur.
	Watch(ctx context.Context, ch chan<- *Event, opts ...WatchOption) error
}

// Entry is an indexed key/value pair
type Entry struct {
	// Index is the unique, monotonically increasing, globally unique index of the entry. The index is static
	// for the lifetime of a key.
	Index Index

	// Version is the unique, monotonically increasing version number for the key/value pair. The version is
	// suitable for use in optimistic locking.
	Version Version

	// Key is the key of the pair
	Key string

	// Value is the value of the pair
	Value []byte

	// Created is the time at which the key was created
	Created time.Time

	// Updated is the time at which the key was last updated
	Updated time.Time
}

func (kv Entry) String() string {
	return fmt.Sprintf("key: %s\nvalue: %s\nversion: %d", kv.Key, string(kv.Value), kv.Version)
}

// EventType is the type of a map event
type EventType string

const (
	// EventNone indicates the event is not a change event
	EventNone EventType = ""

	// EventInserted indicates a key was newly created in the map
	EventInserted EventType = "inserted"

	// EventUpdated indicates the value of an existing key was changed
	EventUpdated EventType = "updated"

	// EventRemoved indicates a key was removed from the map
	EventRemoved EventType = "removed"
)

// Event is a map change event
type Event struct {
	// Type indicates the change event type
	Type EventType

	// Entry is the event entry
	Entry *Entry
}

// New creates a new IndexedMap primitive
func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.Option) (IndexedMap, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}
	return newIndexedMap(ctx, name, partitions[i], opts...)
}

// newIndexedMap creates a new IndexedMap for the given partition
func newIndexedMap(ctx context.Context, name primitive.Name, conn *grpc.ClientConn, opts ...session.Option) (*indexedMap, error) {
	client := api.NewIndexedMapServiceClient(conn)
	sess, err := session.New(ctx, name, &sessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &indexedMap{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

// indexedMap is the default single-partition implementation of Map
type indexedMap struct {
	name    primitive.Name
	client  api.IndexedMapServiceClient
	session *session.Session
}

func (m *indexedMap) Name() primitive.Name {
	return m.name
}

func (m *indexedMap) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry, error) {
	stream, header := m.session.NextStream()
	defer stream.Close()

	request := &api.PutRequest{
		Header: header,
		Key:    key,
		Value:  value,
	}

	for i := range opts {
		opts[i].beforePut(request)
	}

	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterPut(response)
	}

	m.session.RecordResponse(request.Header, response.Header)

	if response.Status == api.ResponseStatus_OK {
		return &Entry{
			Index:   Index(response.Index),
			Key:     key,
			Value:   value,
			Version: Version(response.Header.Index),
		}, nil
	} else if response.Status == api.ResponseStatus_PRECONDITION_FAILED {
		return nil, errors.New("write condition failed")
	} else if response.Status == api.ResponseStatus_WRITE_LOCK {
		return nil, errors.New("write lock failed")
	} else {
		return &Entry{
			Index:   Index(response.Index),
			Key:     key,
			Value:   value,
			Version: Version(response.PreviousVersion),
			Created: response.Created,
			Updated: response.Updated,
		}, nil
	}
}

func (m *indexedMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry, error) {
	request := &api.GetRequest{
		Header: m.session.GetRequest(),
		Key:    key,
	}

	for i := range opts {
		opts[i].beforeGet(request)
	}

	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterGet(response)
	}

	m.session.RecordResponse(request.Header, response.Header)

	if response.Version != 0 {
		return &Entry{
			Index:   Index(response.Index),
			Key:     key,
			Value:   response.Value,
			Version: Version(response.Version),
			Created: response.Created,
			Updated: response.Updated,
		}, nil
	}
	return nil, nil
}

func (m *indexedMap) GetIndex(ctx context.Context, index Index, opts ...GetOption) (*Entry, error) {
	request := &api.GetRequest{
		Header: m.session.GetRequest(),
		Index:  int64(index),
	}

	for i := range opts {
		opts[i].beforeGet(request)
	}

	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterGet(response)
	}

	m.session.RecordResponse(request.Header, response.Header)

	if response.Version != 0 {
		return &Entry{
			Index:   Index(response.Index),
			Key:     response.Key,
			Value:   response.Value,
			Version: Version(response.Version),
			Created: response.Created,
			Updated: response.Updated,
		}, nil
	}
	return nil, nil
}

func (m *indexedMap) Replace(ctx context.Context, key string, value []byte, opts ...ReplaceOption) (*Entry, error) {
	stream, header := m.session.NextStream()
	defer stream.Close()

	request := &api.ReplaceRequest{
		Header:   header,
		Key:      key,
		NewValue: value,
	}

	for i := range opts {
		opts[i].beforeReplace(request)
	}

	response, err := m.client.Replace(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterReplace(response)
	}

	m.session.RecordResponse(request.Header, response.Header)

	if response.Status == api.ResponseStatus_OK {
		return &Entry{
			Index:   Index(response.Index),
			Key:     key,
			Value:   response.PreviousValue,
			Version: Version(response.PreviousVersion),
		}, nil
	} else if response.Status == api.ResponseStatus_PRECONDITION_FAILED {
		return nil, errors.New("write condition failed")
	} else if response.Status == api.ResponseStatus_WRITE_LOCK {
		return nil, errors.New("write lock failed")
	} else {
		return nil, nil
	}
}

func (m *indexedMap) ReplaceIndex(ctx context.Context, index Index, value []byte, opts ...ReplaceOption) (*Entry, error) {
	stream, header := m.session.NextStream()
	defer stream.Close()

	request := &api.ReplaceRequest{
		Header:   header,
		Index:    int64(index),
		NewValue: value,
	}

	for i := range opts {
		opts[i].beforeReplace(request)
	}

	response, err := m.client.Replace(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterReplace(response)
	}

	m.session.RecordResponse(request.Header, response.Header)

	if response.Status == api.ResponseStatus_OK {
		return &Entry{
			Index:   Index(response.Index),
			Key:     response.Key,
			Value:   response.PreviousValue,
			Version: Version(response.PreviousVersion),
		}, nil
	} else if response.Status == api.ResponseStatus_PRECONDITION_FAILED {
		return nil, errors.New("write condition failed")
	} else if response.Status == api.ResponseStatus_WRITE_LOCK {
		return nil, errors.New("write lock failed")
	} else {
		return nil, nil
	}
}

func (m *indexedMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry, error) {
	stream, header := m.session.NextStream()
	defer stream.Close()

	request := &api.RemoveRequest{
		Header: header,
		Key:    key,
	}

	for i := range opts {
		opts[i].beforeRemove(request)
	}

	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterRemove(response)
	}

	m.session.RecordResponse(request.Header, response.Header)

	if response.Status == api.ResponseStatus_OK {
		return &Entry{
			Index:   Index(response.Index),
			Key:     key,
			Value:   response.PreviousValue,
			Version: Version(response.PreviousVersion),
		}, nil
	} else if response.Status == api.ResponseStatus_PRECONDITION_FAILED {
		return nil, errors.New("write condition failed")
	} else if response.Status == api.ResponseStatus_WRITE_LOCK {
		return nil, errors.New("write lock failed")
	} else {
		return nil, nil
	}
}

func (m *indexedMap) RemoveIndex(ctx context.Context, index Index, opts ...RemoveOption) (*Entry, error) {
	stream, header := m.session.NextStream()
	defer stream.Close()

	request := &api.RemoveRequest{
		Header: header,
		Index:  int64(index),
	}

	for i := range opts {
		opts[i].beforeRemove(request)
	}

	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, err
	}

	for i := range opts {
		opts[i].afterRemove(response)
	}

	m.session.RecordResponse(request.Header, response.Header)

	if response.Status == api.ResponseStatus_OK {
		return &Entry{
			Index:   Index(response.Index),
			Key:     response.Key,
			Value:   response.PreviousValue,
			Version: Version(response.PreviousVersion),
		}, nil
	} else if response.Status == api.ResponseStatus_PRECONDITION_FAILED {
		return nil, errors.New("write condition failed")
	} else if response.Status == api.ResponseStatus_WRITE_LOCK {
		return nil, errors.New("write lock failed")
	} else {
		return nil, nil
	}
}

func (m *indexedMap) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Header: m.session.GetRequest(),
	}

	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	m.session.RecordResponse(request.Header, response.Header)
	return int(response.Size_), nil
}

func (m *indexedMap) Clear(ctx context.Context) error {
	stream, header := m.session.NextStream()
	defer stream.Close()

	request := &api.ClearRequest{
		Header: header,
	}

	response, err := m.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	m.session.RecordResponse(request.Header, response.Header)
	return nil
}

func (m *indexedMap) Entries(ctx context.Context, ch chan<- *Entry) error {
	request := &api.EntriesRequest{
		Header: m.session.GetRequest(),
	}
	entries, err := m.client.Entries(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		defer close(ch)
		for {
			response, err := entries.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				glog.Error("Failed to receive entry stream", err)
				break
			}

			// Record the response header
			m.session.RecordResponse(request.Header, response.Header)

			ch <- &Entry{
				Index:   Index(response.Index),
				Key:     response.Key,
				Value:   response.Value,
				Version: Version(response.Version),
				Created: response.Created,
				Updated: response.Updated,
			}
		}
	}()
	return nil
}

func (m *indexedMap) Watch(ctx context.Context, ch chan<- *Event, opts ...WatchOption) error {
	stream, header := m.session.NextStream()

	request := &api.EventRequest{
		Header: header,
	}

	for _, opt := range opts {
		opt.beforeWatch(request)
	}

	events, err := m.client.Events(ctx, request)
	if err != nil {
		stream.Close()
		return err
	}

	openCh := make(chan error)
	go func() {
		defer func() {
			_ = recover()
		}()
		defer close(ch)

		open := false
		for {
			response, err := events.Recv()
			if err == io.EOF {
				if !open {
					close(openCh)
				}
				stream.Close()
				break
			}

			if err != nil {
				glog.Error("Failed to receive event stream", err)
				if !open {
					openCh <- err
					close(openCh)
				}
				stream.Close()
				break
			}

			for _, opt := range opts {
				opt.afterWatch(response)
			}

			// Record the response header
			m.session.RecordResponse(request.Header, response.Header)

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			// Return the Watch call if possible
			if !open {
				close(openCh)
				open = true
			}

			// If this is a normal event (not a handshake response), write the event to the watch channel
			if response.Type != api.EventResponse_OPEN {
				var t EventType
				switch response.Type {
				case api.EventResponse_NONE:
					t = EventNone
				case api.EventResponse_INSERTED:
					t = EventInserted
				case api.EventResponse_UPDATED:
					t = EventUpdated
				case api.EventResponse_REMOVED:
					t = EventRemoved
				}
				ch <- &Event{
					Type: t,
					Entry: &Entry{
						Index:   Index(response.Index),
						Key:     response.Key,
						Value:   response.Value,
						Version: Version(response.Version),
						Created: response.Created,
						Updated: response.Updated,
					},
				}
			}
		}
	}()

	// Block the Watch until the handshake is complete or times out
	select {
	case err := <-openCh:
		return err
	case <-time.After(15 * time.Second):
		return errors.New("handshake timed out")
	}
}

func (m *indexedMap) Close() error {
	return m.session.Close()
}

func (m *indexedMap) Delete() error {
	return m.session.Delete()
}
