// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	"github.com/atomix/runtime/pkg/time"
)

// Option is a map option
type Option[K, V any] interface {
	apply(options *Options[K, V])
}

// Options is map options
type Options[K, V any] struct {
	primitive.Options
	KeyType   generic.Type[K]
	ValueType generic.Type[V]
}

func (o Options[K, V]) Apply(opts ...Option[K, V]) {
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

func WithTags[K, V any](tags map[string]string) Option[K, V] {
	return newFuncOption[K, V](func(options *Options[K, V]) {
		primitive.WithTags(tags)(&options.Options)
	})
}

func WithTag[K, V any](key, value string) Option[K, V] {
	return newFuncOption[K, V](func(options *Options[K, V]) {
		primitive.WithTag(key, value)(&options.Options)
	})
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

// UpdateOption is an option for the Update method
type UpdateOption interface {
	beforeUpdate(request *mapv1.UpdateRequest)
	afterUpdate(response *mapv1.UpdateResponse)
}

// RemoveOption is an option for the Remove method
type RemoveOption interface {
	beforeRemove(request *mapv1.RemoveRequest)
	afterRemove(response *mapv1.RemoveResponse)
}

// IfTimestamp sets the required version for optimistic concurrency control
func IfTimestamp(timestamp time.Timestamp) TimestampOption {
	return TimestampOption{timestamp: timestamp}
}

// TimestampOption is an implementation of PutOption and RemoveOption to specify the version for concurrency control
type TimestampOption struct {
	PutOption
	UpdateOption
	RemoveOption
	timestamp time.Timestamp
}

func (o TimestampOption) beforePut(request *mapv1.PutRequest) {
	timestamp := o.timestamp.Scheme().Codec().EncodeTimestamp(o.timestamp)
	request.IfTimestamp = &timestamp
}

func (o TimestampOption) afterPut(response *mapv1.PutResponse) {

}

func (o TimestampOption) beforeUpdate(request *mapv1.UpdateRequest) {
	timestamp := o.timestamp.Scheme().Codec().EncodeTimestamp(o.timestamp)
	request.IfTimestamp = &timestamp
}

func (o TimestampOption) afterUpdate(response *mapv1.UpdateResponse) {

}

func (o TimestampOption) beforeRemove(request *mapv1.RemoveRequest) {
	timestamp := o.timestamp.Scheme().Codec().EncodeTimestamp(o.timestamp)
	request.IfTimestamp = &timestamp
}

func (o TimestampOption) afterRemove(response *mapv1.RemoveResponse) {

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
