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

package _map //nolint:golint

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/map"
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
)

// Option is a map option
type Option interface {
	primitive.Option
	applyNewMap(options *newMapOptions)
}

// newMapOptions is map options
type newMapOptions struct{}

// PutOption is an option for the Put method
type PutOption interface {
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

// MatchOption is an implementation of PutOption and RemoveOption to specify the version for concurrency control
type MatchOption struct {
	PutOption
	RemoveOption
	object meta.Object
}

func (o MatchOption) beforePut(request *api.PutRequest) {
	proto := o.object.Meta().Proto()
	request.Preconditions = append(request.Preconditions, api.Precondition{
		Precondition: &api.Precondition_Metadata{
			Metadata: &proto,
		},
	})
}

func (o MatchOption) afterPut(response *api.PutResponse) {

}

func (o MatchOption) beforeRemove(request *api.RemoveRequest) {
	proto := o.object.Meta().Proto()
	request.Preconditions = append(request.Preconditions, api.Precondition{
		Precondition: &api.Precondition_Metadata{
			Metadata: &proto,
		},
	})
}

func (o MatchOption) afterRemove(response *api.RemoveResponse) {

}

// IfNotSet sets the value if the entry is not yet set
func IfNotSet() PutOption {
	return &NotSetOption{}
}

// NotSetOption is a PutOption that sets the value only if it's not already set
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
		request.Key = o.filter.Key
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
	Key string
}
