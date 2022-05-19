// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"github.com/atomix/go-client/pkg/atomix/generic"
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/meta"
)

// Option is a map option
type Option[K, V any] interface {
	apply(options *Options[K, V])
}

// Options is map options
type Options[K, V any] struct {
	KeyType   generic.Type[K]
	ValueType generic.Type[V]
}

func (o Options[K, V]) apply(opts ...Option[K, V]) {
	for _, opt := range opts {
		opt.apply(&o)
	}
}

func newFuncOption[K, V any](f func(*Options[K, V])) Option[K, V] {
	return funcOption[K, V]{f}
}

type funcOption[K, V any] struct {
	f func(*Options[K, V])
}

func (o funcOption[K, V]) apply(options *Options[K, V]) {
	o.f(options)
}

func WithKeyType[K, V any](keyType generic.Type[K]) Option[K, V] {
	return newFuncOption[K, V](func(options *Options[K, V]) {
		options.KeyType = keyType
	})
}

func WithValueType[K, V any](valueType generic.Type[V]) Option[K, V] {
	return newFuncOption[K, V](func(options *Options[K, V]) {
		options.ValueType = valueType
	})
}

// PutOption is an option for the Put method
type PutOption interface {
	beforePut(request *mapv1.PutRequest)
	afterPut(response *mapv1.PutResponse)
}

// RemoveOption is an option for the Remove method
type RemoveOption interface {
	beforeRemove(request *mapv1.RemoveRequest)
	afterRemove(response *mapv1.RemoveResponse)
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

func (o MatchOption) beforePut(request *mapv1.PutRequest) {
	proto := o.object.Meta().Proto()
	request.Preconditions = append(request.Preconditions, mapv1.Precondition{
		Precondition: &mapv1.Precondition_Metadata{
			Metadata: &proto,
		},
	})
}

func (o MatchOption) afterPut(response *mapv1.PutResponse) {

}

func (o MatchOption) beforeRemove(request *mapv1.RemoveRequest) {
	proto := o.object.Meta().Proto()
	request.Preconditions = append(request.Preconditions, mapv1.Precondition{
		Precondition: &mapv1.Precondition_Metadata{
			Metadata: &proto,
		},
	})
}

func (o MatchOption) afterRemove(response *mapv1.RemoveResponse) {

}

// IfNotSet sets the value if the entry is not yet set
func IfNotSet() PutOption {
	return &NotSetOption{}
}

// NotSetOption is a PutOption that sets the value only if it's not already set
type NotSetOption struct {
}

func (o NotSetOption) beforePut(request *mapv1.PutRequest) {
	request.Preconditions = append(request.Preconditions, mapv1.Precondition{
		Precondition: &mapv1.Precondition_Metadata{
			Metadata: &runtimev1.ObjectMeta{
				Type: runtimev1.ObjectMeta_TOMBSTONE,
			},
		},
	})
}

func (o NotSetOption) afterPut(response *mapv1.PutResponse) {

}

// GetOption is an option for the Get method
type GetOption interface {
	beforeGet(request *mapv1.GetRequest)
	afterGet(response *mapv1.GetResponse)
}

// WatchOption is an option for the Watch method
type WatchOption interface {
	beforeWatch(request *mapv1.EventsRequest)
	afterWatch(response *mapv1.EventsResponse)
}

// WithReplay returns a watch option that enables replay of watch events
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *mapv1.EventsRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *mapv1.EventsResponse) {

}

type filterOption struct {
	filter Filter
}

func (o filterOption) beforeWatch(request *mapv1.EventsRequest) {
	if o.filter.Key != "" {
		request.Key = o.filter.Key
	}
}

func (o filterOption) afterWatch(response *mapv1.EventsResponse) {
}

// WithFilter returns a watch option that filters the watch events
func WithFilter(filter Filter) WatchOption {
	return filterOption{filter: filter}
}

// Filter is a watch filter configuration
type Filter struct {
	Key string
}
