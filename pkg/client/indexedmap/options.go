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
	api "github.com/atomix/api/go/atomix/primitive/indexedmap"
	"github.com/atomix/go-client/pkg/client/meta"
	"github.com/google/uuid"
)

// Option is a map option
type Option interface {
	apply(options *options)
}

// options is map options
type options struct {
	clientID string
}

func applyOptions(opts ...Option) options {
	id, err := uuid.NewUUID()
	if err != nil {
		panic(err)
	}
	options := &options{
		clientID: id.String(),
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	return *options
}

// WithClientID sets the client identifier
func WithClientID(id string) Option {
	return &clientIDOption{
		clientID: id,
	}
}

type clientIDOption struct {
	clientID string
}

func (o *clientIDOption) apply(options *options) {
	options.clientID = o.clientID
}

// SetOption is an option for the Put method
type SetOption interface {
	beforePut(input *api.PutInput)
	afterPut(output *api.PutOutput)
}

// RemoveOption is an option for the Remove method
type RemoveOption interface {
	beforeRemove(input *api.RemoveInput)
	afterRemove(output *api.RemoveOutput)
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

func (o MatchOption) beforePut(input *api.PutInput) {
	input.Entry.Value.Meta = o.object.Meta().Proto()
}

func (o MatchOption) afterPut(output *api.PutOutput) {

}

func (o MatchOption) beforeRemove(input *api.RemoveInput) {
	input.Entry.Value.Meta = o.object.Meta().Proto()
}

func (o MatchOption) afterRemove(output *api.RemoveOutput) {

}

// IfNotSet sets the value if the entry is not yet set
func IfNotSet() SetOption {
	return &NotSetOption{}
}

// NotSetOption is a SetOption that sets the value only if it's not already set
type NotSetOption struct {
}

func (o NotSetOption) beforePut(input *api.PutInput) {
	input.IfEmpty = true
}

func (o NotSetOption) afterPut(output *api.PutOutput) {

}

// GetOption is an option for the Get method
type GetOption interface {
	beforeGet(input *api.GetInput)
	afterGet(output *api.GetOutput)
}

// WatchOption is an option for the Watch method
type WatchOption interface {
	beforeWatch(input *api.EventsInput)
	afterWatch(output *api.EventsOutput)
}

// WithReplay returns a watch option that enables replay of watch events
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(input *api.EventsInput) {
	input.Replay = true
}

func (o replayOption) afterWatch(output *api.EventsOutput) {

}

type filterOption struct {
	filter Filter
}

func (o filterOption) beforeWatch(input *api.EventsInput) {
	if o.filter.Key != "" {
		input.Pos.Key = o.filter.Key
	}
	if o.filter.Index > 0 {
		input.Pos.Index = uint64(o.filter.Index)
	}
}

func (o filterOption) afterWatch(output *api.EventsOutput) {
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
