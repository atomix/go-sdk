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

package list

import (
	"context"
	api "github.com/atomix/atomix-api/proto/atomix/list"
	"github.com/atomix/atomix-go-client/pkg/client/primitive"
	"github.com/atomix/atomix-go-client/pkg/client/session"
	"github.com/atomix/atomix-go-client/pkg/client/util"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
)

type ListClient interface {
	GetList(ctx context.Context, name string, opts ...session.SessionOption) (List, error)
}

type List interface {
	primitive.Primitive
	Append(ctx context.Context, value string) error
	Insert(ctx context.Context, index int, value string) error
	Get(ctx context.Context, index int) (string, error)
	Remove(ctx context.Context, index int) (string, error)
	Len(ctx context.Context) (int, error)
	Items(ctx context.Context, ch chan<- string) error
	Watch(ctx context.Context, ch chan<- *ListEvent, opts ...WatchOption) error
	Clear(ctx context.Context) error
}

type ListEventType string

const (
	EventNone     ListEventType = ""
	EventInserted ListEventType = "added"
	EventRemoved  ListEventType = "removed"
)

type ListEvent struct {
	Type  ListEventType
	Index int
	Value string
}

func New(ctx context.Context, name primitive.Name, partitions []*grpc.ClientConn, opts ...session.SessionOption) (List, error) {
	i, err := util.GetPartitionIndex(name.Name, len(partitions))
	if err != nil {
		return nil, err
	}
	return newList(ctx, name, partitions[i], opts...)
}

func newList(ctx context.Context, name primitive.Name, conn *grpc.ClientConn, opts ...session.SessionOption) (*list, error) {
	client := api.NewListServiceClient(conn)
	sess, err := session.New(ctx, name, &SessionHandler{client: client}, opts...)
	if err != nil {
		return nil, err
	}
	return &list{
		name:    name,
		client:  client,
		session: sess,
	}, nil
}

type list struct {
	name    primitive.Name
	client  api.ListServiceClient
	session *session.Session
}

func (l *list) Name() primitive.Name {
	return l.name
}

func (l *list) Append(ctx context.Context, value string) error {
	request := &api.AppendRequest{
		Header: l.session.NextRequest(),
		Value:  value,
	}

	response, err := l.client.Append(ctx, request)
	if err != nil {
		return err
	}

	l.session.RecordResponse(request.Header, response.Header)
	return err
}

func (l *list) Insert(ctx context.Context, index int, value string) error {
	request := &api.InsertRequest{
		Header: l.session.NextRequest(),
		Index:  uint32(index),
		Value:  value,
	}

	response, err := l.client.Insert(ctx, request)
	if err != nil {
		return err
	}

	l.session.RecordResponse(request.Header, response.Header)
	return err
}

func (l *list) Get(ctx context.Context, index int) (string, error) {
	request := &api.GetRequest{
		Header: l.session.GetRequest(),
		Index:  uint32(index),
	}

	response, err := l.client.Get(ctx, request)
	if err != nil {
		return "", err
	}

	l.session.RecordResponse(request.Header, response.Header)
	return response.Value, nil
}

func (l *list) Remove(ctx context.Context, index int) (string, error) {
	request := &api.RemoveRequest{
		Header: l.session.NextRequest(),
		Index:  uint32(index),
	}

	response, err := l.client.Remove(ctx, request)
	if err != nil {
		return "", err
	}

	l.session.RecordResponse(request.Header, response.Header)
	return response.Value, nil
}

func (l *list) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Header: l.session.GetRequest(),
	}

	response, err := l.client.Size(ctx, request)
	if err != nil {
		return 0, err
	}

	l.session.RecordResponse(request.Header, response.Header)
	return int(response.Size_), nil
}

func (l *list) Items(ctx context.Context, ch chan<- string) error {
	request := &api.IterateRequest{
		Header: l.session.GetRequest(),
	}
	entries, err := l.client.Iterate(ctx, request)
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
				glog.Error("Failed to receive items stream", err)
				break
			}

			// Record the response header
			l.session.RecordResponse(request.Header, response.Header)

			ch <- response.Value
		}
	}()
	return nil
}

func (l *list) Watch(ctx context.Context, ch chan<- *ListEvent, opts ...WatchOption) error {
	request := &api.EventRequest{
		Header: l.session.NextRequest(),
	}

	for _, opt := range opts {
		opt.beforeWatch(request)
	}

	events, err := l.client.Events(ctx, request)
	if err != nil {
		return err
	}

	go func() {
		defer close(ch)
		var stream *session.Stream
		for {
			response, err := events.Recv()
			if err == io.EOF {
				if stream != nil {
					stream.Close()
				}
				break
			}

			if err != nil {
				glog.Error("Failed to receive event stream", err)
				break
			}

			for _, opt := range opts {
				opt.afterWatch(response)
			}

			// Record the response header
			l.session.RecordResponse(request.Header, response.Header)

			// Initialize the session stream if necessary.
			if stream == nil {
				stream = l.session.NewStream(response.Header.StreamID)
			}

			// Attempt to serialize the response to the stream and skip the response if serialization failed.
			if !stream.Serialize(response.Header) {
				continue
			}

			var t ListEventType
			switch response.Type {
			case api.EventResponse_NONE:
				t = EventNone
			case api.EventResponse_ADDED:
				t = EventInserted
			case api.EventResponse_REMOVED:
				t = EventRemoved
			}

			ch <- &ListEvent{
				Type:  t,
				Index: int(response.Index),
				Value: response.Value,
			}
		}
	}()
	return nil
}

func (l *list) Clear(ctx context.Context) error {
	request := &api.ClearRequest{
		Header: l.session.NextRequest(),
	}

	response, err := l.client.Clear(ctx, request)
	if err != nil {
		return err
	}

	l.session.RecordResponse(request.Header, response.Header)
	return nil
}

func (l *list) Close() error {
	return l.session.Close()
}

func (l *list) Delete() error {
	return l.session.Delete()
}
