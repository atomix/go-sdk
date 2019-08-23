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
	api "github.com/atomix/atomix-api/proto/atomix/map"
)

type PutOption interface {
	beforePut(request *api.PutRequest)
	afterPut(response *api.PutResponse)
}

type RemoveOption interface {
	beforeRemove(request *api.RemoveRequest)
	afterRemove(response *api.RemoveResponse)
}

func WithVersion(version int64) VersionOption {
	return VersionOption{version: version}
}

type VersionOption struct {
	PutOption
	RemoveOption
	version int64
}

func (o VersionOption) beforePut(request *api.PutRequest) {
	request.Version = o.version
}

func (o VersionOption) afterPut(response *api.PutResponse) {

}

func (o VersionOption) beforeRemove(request *api.RemoveRequest) {
	request.Version = o.version
}

func (o VersionOption) afterRemove(response *api.RemoveResponse) {

}

type GetOption interface {
	beforeGet(request *api.GetRequest)
	afterGet(response *api.GetResponse)
}

func WithDefault(def []byte) GetOption {
	return defaultOption{def: def}
}

type defaultOption struct {
	def []byte
}

func (o defaultOption) beforeGet(request *api.GetRequest) {
}

func (o defaultOption) afterGet(response *api.GetResponse) {
	if response.Version == 0 {
		response.Value = o.def
	}
}

// WatchOption is an option for a map Watch request
type WatchOption interface {
	beforeWatch(request *api.EventRequest)
	afterWatch(response *api.EventResponse)
}

// WithReplay returns a watch option that enables replay of watch events
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *api.EventRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *api.EventResponse) {

}
