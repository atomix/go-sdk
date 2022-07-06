// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/time"
)

// Option is a value option
type Option[V any] interface {
	apply(options *Options[V])
}

// Options is value options
type Options[V any] struct {
	primitive.Options
	ValueType generic.Type[V]
}

func (o Options[V]) Apply(opts ...Option[V]) {
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

func WithTags[V any](tags map[string]string) Option[V] {
	return newFuncOption[V](func(options *Options[V]) {
		primitive.WithTags(tags)(&options.Options)
	})
}

func WithTag[V any](key, value string) Option[V] {
	return newFuncOption[V](func(options *Options[V]) {
		primitive.WithTag(key, value)(&options.Options)
	})
}

func WithType[V any](valueType generic.Type[V]) Option[V] {
	return newFuncOption[V](func(options *Options[V]) {
		options.ValueType = valueType
	})
}

// SetOption is an option for Set calls
type SetOption interface {
	beforeSet(request *valuev1.SetRequest)
	afterSet(response *valuev1.SetResponse)
}

// IfTimestamp updates the value if the version matches the given version
func IfTimestamp(timestamp time.Timestamp) SetOption {
	return timestampOption{timestamp}
}

type timestampOption struct {
	timestamp time.Timestamp
}

func (o timestampOption) beforeSet(request *valuev1.SetRequest) {
	timestamp := o.timestamp.Scheme().Codec().EncodeTimestamp(o.timestamp)
	request.IfTimestamp = &timestamp
}

func (o timestampOption) afterSet(response *valuev1.SetResponse) {

}
