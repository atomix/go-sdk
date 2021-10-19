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

package set

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/set/v1"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
)

// Option is a set option
type Option interface {
	primitive.Option
	applyNewSet(options *newSetOptions)
}

// newSetOptions is set options
type newSetOptions struct {
	sessionOptions api.SetSessionOptions
}

type CacheOption struct {
	primitive.EmptyOption
	options api.SetCacheOptions
}

func (o *CacheOption) applyNewSet(options *newSetOptions) {
	options.sessionOptions.Cache = o.options
}

// WithNearCache enabled a near cache for a set
func WithNearCache() Option {
	return &CacheOption{
		options: api.SetCacheOptions{
			Enabled:  true,
			Strategy: api.SetCacheStrategy_NEAR,
		},
	}
}

// WithReadThroughCache enabled a read-through cache for a set
func WithReadThroughCache() Option {
	return &CacheOption{
		options: api.SetCacheOptions{
			Enabled:  true,
			Strategy: api.SetCacheStrategy_READ_THROUGH,
		},
	}
}

// WithWriteThroughCache enabled a write-through cache for a set
func WithWriteThroughCache() Option {
	return &CacheOption{
		options: api.SetCacheOptions{
			Enabled:  true,
			Strategy: api.SetCacheStrategy_WRITE_THROUGH,
		},
	}
}

// WithReadThroughWriteThroughCache enabled a read-through/write-through cache for a set
func WithReadThroughWriteThroughCache() Option {
	return &CacheOption{
		options: api.SetCacheOptions{
			Enabled:  true,
			Strategy: api.SetCacheStrategy_READ_THROUGH_WRITE_THROUGH,
		},
	}
}

// WatchOption is an option for set Watch calls
type WatchOption interface {
	beforeWatch(request *api.EventsRequest)
	afterWatch(response *api.EventsResponse)
}

// WithReplay returns a Watch option to replay entries
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *api.EventsRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *api.EventsResponse) {

}
