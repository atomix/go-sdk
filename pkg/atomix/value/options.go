// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/value"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"github.com/atomix/go-client/pkg/atomix/primitive/codec"
)

// Option is a value option
type Option[V any] interface {
	primitive.Option
	applyNewValue(options *newValueOptions[V])
}

// newValueOptions is value options
type newValueOptions[V any] struct {
	valueCodec codec.Codec[V]
}

func WithCodec[E any](valueCodec codec.Codec[E]) Option[E] {
	return codecOption[E]{
		valueCodec: valueCodec,
	}
}

type codecOption[V any] struct {
	primitive.EmptyOption
	valueCodec codec.Codec[V]
}

func (o codecOption[V]) applyNewValue(options *newValueOptions[V]) {
	options.valueCodec = o.valueCodec
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
