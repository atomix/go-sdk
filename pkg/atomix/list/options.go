// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	listv1 "github.com/atomix/runtime/api/atomix/runtime/list/v1"
)

// Option is a map option
type Option[E any] interface {
	apply(options *Options[E])
}

// Options is map options
type Options[E any] struct {
	primitive.Options
	ElementType generic.Type[E]
}

func (o Options[E]) Apply(opts ...Option[E]) {
	for _, opt := range opts {
		opt.apply(&o)
	}
}

func newFuncOption[E any](f func(*Options[E])) Option[E] {
	return funcOption[E]{f}
}

type funcOption[E any] struct {
	f func(*Options[E])
}

func (o funcOption[E]) apply(options *Options[E]) {
	o.f(options)
}

func WithTags[E any](tags map[string]string) Option[E] {
	return newFuncOption[E](func(options *Options[E]) {
		primitive.WithTags(tags)(&options.Options)
	})
}

func WithTag[E any](key, value string) Option[E] {
	return newFuncOption[E](func(options *Options[E]) {
		primitive.WithTag(key, value)(&options.Options)
	})
}

func WithElementType[E any](elementType generic.Type[E]) Option[E] {
	return newFuncOption[E](func(options *Options[E]) {
		options.ElementType = elementType
	})
}

// WatchOption is an option for list Watch calls
type WatchOption interface {
	beforeWatch(request *listv1.EventsRequest)
	afterWatch(response *listv1.EventsResponse)
}

// WithReplay returns a Watch option to replay entries
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *listv1.EventsRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *listv1.EventsResponse) {

}
