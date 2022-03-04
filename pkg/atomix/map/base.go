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

package _map

import (
	"context"
	api "github.com/atomix/atomix-api/go/atomix/primitive/map"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"io"
)

type baseMap struct {
	*primitive.Client[Map]
	client api.MapServiceClient
}

func (m *baseMap) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (*Entry[string, []byte], error) {
	request := &api.PutRequest{
		Headers: m.GetHeaders(),
		Entry: api.Entry{
			Key: api.Key{
				Key: key,
			},
			Value: &api.Value{
				Value: value,
			},
		},
	}
	for i := range opts {
		opts[i].beforePut(request)
	}
	response, err := m.client.Put(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	for i := range opts {
		opts[i].afterPut(response)
	}
	return newEntry(&response.Entry), nil
}

func (m *baseMap) Get(ctx context.Context, key string, opts ...GetOption) (*Entry[string, []byte], error) {
	request := &api.GetRequest{
		Headers: m.GetHeaders(),
		Key:     key,
	}
	for i := range opts {
		opts[i].beforeGet(request)
	}
	response, err := m.client.Get(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	for i := range opts {
		opts[i].afterGet(response)
	}
	return newEntry(&response.Entry), nil
}

func (m *baseMap) Remove(ctx context.Context, key string, opts ...RemoveOption) (*Entry[string, []byte], error) {
	request := &api.RemoveRequest{
		Headers: m.GetHeaders(),
		Key: api.Key{
			Key: key,
		},
	}
	for i := range opts {
		opts[i].beforeRemove(request)
	}
	response, err := m.client.Remove(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	for i := range opts {
		opts[i].afterRemove(response)
	}
	return newEntry(&response.Entry), nil
}

func (m *baseMap) Len(ctx context.Context) (int, error) {
	request := &api.SizeRequest{
		Headers: m.GetHeaders(),
	}
	response, err := m.client.Size(ctx, request)
	if err != nil {
		return 0, errors.From(err)
	}
	return int(response.Size_), nil
}

func (m *baseMap) Clear(ctx context.Context) error {
	request := &api.ClearRequest{
		Headers: m.GetHeaders(),
	}
	_, err := m.client.Clear(ctx, request)
	if err != nil {
		return errors.From(err)
	}
	return nil
}

func (m *baseMap) Entries(ctx context.Context, ch chan<- Entry[string, []byte]) error {
	request := &api.EntriesRequest{
		Headers: m.GetHeaders(),
	}
	stream, err := m.client.Entries(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	go func() {
		defer close(ch)
		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				err = errors.From(err)
				if errors.IsCanceled(err) || errors.IsTimeout(err) {
					return
				}
				log.Errorf("Entries failed: %v", err)
				return
			}
			ch <- *newEntry(&response.Entry)
		}
	}()
	return nil
}

func (m *baseMap) Watch(ctx context.Context, ch chan<- Event[string, []byte], opts ...WatchOption) error {
	request := &api.EventsRequest{
		Headers: m.GetHeaders(),
	}
	for i := range opts {
		opts[i].beforeWatch(request)
	}

	stream, err := m.client.Events(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	openCh := make(chan struct{})
	go func() {
		defer close(ch)
		open := false
		defer func() {
			if !open {
				close(openCh)
			}
		}()
		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				err = errors.From(err)
				if errors.IsCanceled(err) || errors.IsTimeout(err) {
					return
				}
				log.Errorf("Watch failed: %v", err)
				return
			}

			if !open {
				close(openCh)
				open = true
			}

			for i := range opts {
				opts[i].afterWatch(response)
			}

			switch response.Event.Type {
			case api.Event_INSERT:
				ch <- Event[string, []byte]{
					Type:  EventInsert,
					Entry: *newEntry(&response.Event.Entry),
				}
			case api.Event_UPDATE:
				ch <- Event[string, []byte]{
					Type:  EventUpdate,
					Entry: *newEntry(&response.Event.Entry),
				}
			case api.Event_REMOVE:
				ch <- Event[string, []byte]{
					Type:  EventRemove,
					Entry: *newEntry(&response.Event.Entry),
				}
			case api.Event_REPLAY:
				ch <- Event[string, []byte]{
					Type:  EventReplay,
					Entry: *newEntry(&response.Event.Entry),
				}
			}
		}
	}()

	select {
	case <-openCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newEntry(entry *api.Entry) *Entry[string, []byte] {
	if entry == nil {
		return nil
	}
	return &Entry[string, []byte]{
		ObjectMeta: meta.FromProto(entry.Key.ObjectMeta),
		Key:        entry.Key.Key,
		Value:      entry.Value.Value,
	}
}
