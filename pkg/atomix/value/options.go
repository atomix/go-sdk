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

package value

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/value/v1"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-sdk-go/pkg/meta"
)

// Option is a value option
type Option interface {
	primitive.Option
	applyNewValue(options *newValueOptions)
}

// newValueOptions is value options
type newValueOptions struct {
	sessionOptions api.ValueSessionOptions
}

type CacheOption struct {
	primitive.EmptyOption
	options api.ValueCacheOptions
}

func (o *CacheOption) applyNewValue(options *newValueOptions) {
	options.sessionOptions.Cache = o.options
}

// WithNearCache enabled a near cache for a value
func WithNearCache() Option {
	return &CacheOption{
		options: api.ValueCacheOptions{
			Enabled:  true,
			Strategy: api.ValueCacheStrategy_NEAR,
		},
	}
}

// WithReadThroughCache enabled a read-through cache for a value
func WithReadThroughCache() Option {
	return &CacheOption{
		options: api.ValueCacheOptions{
			Enabled:  true,
			Strategy: api.ValueCacheStrategy_READ_THROUGH,
		},
	}
}

// WithWriteThroughCache enabled a write-through cache for a value
func WithWriteThroughCache() Option {
	return &CacheOption{
		options: api.ValueCacheOptions{
			Enabled:  true,
			Strategy: api.ValueCacheStrategy_WRITE_THROUGH,
		},
	}
}

// WithReadThroughWriteThroughCache enabled a read-through/write-through cache for a value
func WithReadThroughWriteThroughCache() Option {
	return &CacheOption{
		options: api.ValueCacheOptions{
			Enabled:  true,
			Strategy: api.ValueCacheStrategy_READ_THROUGH_WRITE_THROUGH,
		},
	}
}

// SetOption is an option for Set calls
type SetOption interface {
	beforeSet(request *api.SetRequest)
	afterSet(response *api.SetResponse)
}

// IfMatch updates the value if the version matches the given version
func IfMatch(object meta.Object) SetOption {
	return matchOption{object}
}

type matchOption struct {
	object meta.Object
}

func (o matchOption) beforeSet(request *api.SetRequest) {
	proto := o.object.Meta().Proto()
	request.Preconditions = append(request.Preconditions, api.Precondition{
		Precondition: &api.Precondition_Metadata{
			Metadata: &proto,
		},
	})
}

func (o matchOption) afterSet(response *api.SetResponse) {

}
