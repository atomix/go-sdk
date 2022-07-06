// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"github.com/atomix/go-client/pkg/atomix/generic"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
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

// UpdateOption is an option for the Update method
type UpdateOption interface {
	beforeUpdate(request *indexedmapv1.UpdateRequest)
	afterUpdate(response *indexedmapv1.UpdateResponse)
}

// RemoveOption is an option for the Remove method
type RemoveOption interface {
	beforeRemove(request *indexedmapv1.RemoveRequest)
	afterRemove(response *indexedmapv1.RemoveResponse)
}

// IfTimestamp sets the required version for optimistic concurrency control
func IfTimestamp(timestamp time.Timestamp) TimestampOption {
	return TimestampOption{timestamp: timestamp}
}

// TimestampOption is an implementation of SetOption and RemoveOption to specify the version for concurrency control
type TimestampOption struct {
	UpdateOption
	RemoveOption
	timestamp time.Timestamp
}

func (o TimestampOption) beforeUpdate(request *indexedmapv1.UpdateRequest) {
	timestamp := o.timestamp.Scheme().Codec().EncodeTimestamp(o.timestamp)
	request.IfTimestamp = &timestamp
}

func (o TimestampOption) afterUpdate(response *indexedmapv1.UpdateResponse) {

}

func (o TimestampOption) beforeRemove(request *indexedmapv1.RemoveRequest) {
	timestamp := o.timestamp.Scheme().Codec().EncodeTimestamp(o.timestamp)
	request.IfTimestamp = &timestamp
}

func (o TimestampOption) afterRemove(response *indexedmapv1.RemoveResponse) {

}

// GetOption is an option for the Get method
type GetOption interface {
	beforeGet(request *indexedmapv1.GetRequest)
	afterGet(response *indexedmapv1.GetResponse)
}

// WatchOption is an option for the Watch method
type WatchOption interface {
	beforeWatch(request *indexedmapv1.EventsRequest)
	afterWatch(response *indexedmapv1.EventsResponse)
}

// WithReplay returns a watch option that enables replay of watch events
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *indexedmapv1.EventsRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *indexedmapv1.EventsResponse) {

}

type filterOption struct {
	key string
}

func (o filterOption) beforeWatch(request *indexedmapv1.EventsRequest) {
	if o.key != "" {
		request.Key = o.key
	}
}

func (o filterOption) afterWatch(response *indexedmapv1.EventsResponse) {
}

// WithFilterKey returns a watch option that filters the watch events
func WithFilterKey(key string) WatchOption {
	return filterOption{key: key}
}
