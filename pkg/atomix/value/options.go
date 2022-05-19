// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"github.com/atomix/go-client/pkg/atomix/generic"
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/meta"
)

// Option is a value option
type Option[V any] interface {
	apply(options *Options[V])
}

// Options is value options
type Options[V any] struct {
	ValueType generic.Type[V]
}

func (o Options[V]) apply(opts ...Option[V]) {
	for _, opt := range opts {
		opt.apply(&o)
	}
}

func newFuncOption[V any](f func(*Options[V])) Option[V] {
	return funcOption[V]{f}
}

type funcOption[V any] struct {
	f func(*Options[V])
}

func (o funcOption[V]) apply(options *Options[V]) {
	o.f(options)
}

func WithValueType[V any](valueType generic.Type[V]) Option[V] {
	return newFuncOption[V](func(options *Options[V]) {
		options.ValueType = valueType
	})
}

// SetOption is an option for Set calls
type SetOption interface {
	beforeSet(request *valuev1.SetRequest)
	afterSet(response *valuev1.SetResponse)
}

// IfMatch updates the value if the version matches the given version
func IfMatch(object meta.Object) SetOption {
	return matchOption{object}
}

type matchOption struct {
	object meta.Object
}

func (o matchOption) beforeSet(request *valuev1.SetRequest) {
	proto := o.object.Meta().Proto()
	request.Preconditions = append(request.Preconditions, valuev1.Precondition{
		Precondition: &valuev1.Precondition_Metadata{
			Metadata: &proto,
		},
	})
}

func (o matchOption) afterSet(response *valuev1.SetResponse) {

}
