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
	api "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
)

// Option is a value option
type Option interface {
	primitive.Option
	applyNewValue(options *newValueOptions)
}

// newValueOptions is value options
type newValueOptions struct{}

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
	request.Value.ObjectMeta = o.object.Meta().Proto()
}

func (o matchOption) afterSet(response *api.SetResponse) {

}
