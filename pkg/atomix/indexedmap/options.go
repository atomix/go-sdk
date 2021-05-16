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
	api "github.com/atomix/atomix-api/go/atomix/primitive/indexedmap"
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
)

// Option is a indexed map option
type Option interface {
	primitive.Option
	applyNewIndexedMap(options *newIndexedMapOptions)
}

// newIndexedMapOptions is indexed map options
type newIndexedMapOptions struct {
	clientID string
}

// WithClientID sets the client identifier
func WithClientID(id string) Option {
	return &clientIDOption{
		clientID: id,
	}
}

type clientIDOption struct {
	primitive.EmptyOption
	clientID string
}

func (o *clientIDOption) applyNewIndexedMap(options *newIndexedMapOptions) {
	options.clientID = o.clientID
}

// SetOption is an option for the Put method
type SetOption interface {
	beforePut(request *api.PutRequest)
	afterPut(response *api.PutResponse)
}

// RemoveOption is an option for the Remove method
type RemoveOption interface {
	beforeRemove(request *api.RemoveRequest)
	afterRemove(response *api.RemoveResponse)
}

// IfMatch sets the required version for optimistic concurrency control
func IfMatch(object meta.Object) MatchOption {
	return MatchOption{object: object}
}

// MatchOption is an implementation of SetOption and RemoveOption to specify the version for concurrency control
type MatchOption struct {
	SetOption
	RemoveOption
	object meta.Object
}

func (o MatchOption) beforePut(request *api.PutRequest) {
	request.Entry.Value.ObjectMeta = o.object.Meta().Proto()
}

func (o MatchOption) afterPut(response *api.PutResponse) {

}

func (o MatchOption) beforeRemove(request *api.RemoveRequest) {
	request.Entry.Value.ObjectMeta = o.object.Meta().Proto()
}

func (o MatchOption) afterRemove(response *api.RemoveResponse) {

}

// IfNotSet sets the value if the entry is not yet set
func IfNotSet() SetOption {
	return &NotSetOption{}
}

// NotSetOption is a SetOption that sets the value only if it's not already set
type NotSetOption struct {
}

func (o NotSetOption) beforePut(request *api.PutRequest) {
	request.Preconditions = append(request.Preconditions, api.Precondition{
		Precondition: &api.Precondition_Metadata{
			Metadata: &metaapi.ObjectMeta{
				Type: metaapi.ObjectMeta_TOMBSTONE,
			},
		},
	})
}

func (o NotSetOption) afterPut(response *api.PutResponse) {

}

// GetOption is an option for the Get method
type GetOption interface {
	beforeGet(request *api.GetRequest)
	afterGet(response *api.GetResponse)
}

// WatchOption is an option for the Watch method
type WatchOption interface {
	beforeWatch(request *api.EventsRequest)
	afterWatch(response *api.EventsResponse)
}

// WithReplay returns a watch option that enables replay of watch events
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *api.EventsRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *api.EventsResponse) {

}

type filterOption struct {
	filter Filter
}

func (o filterOption) beforeWatch(request *api.EventsRequest) {
	if o.filter.Key != "" {
		request.Pos.Key = o.filter.Key
	}
	if o.filter.Index > 0 {
		request.Pos.Index = uint64(o.filter.Index)
	}
}

func (o filterOption) afterWatch(response *api.EventsResponse) {
}

// WithFilter returns a watch option that filters the watch events
func WithFilter(filter Filter) WatchOption {
	return filterOption{filter: filter}
}

// Filter is a watch filter configuration
type Filter struct {
	Key   string
	Index Index
}
