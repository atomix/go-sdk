// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"github.com/atomix/go-client/pkg/atomix/generic"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/time"
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

// SetOption is an option for the Put method
type SetOption interface {
	beforePut(request *indexedmapv1.PutRequest)
	afterPut(response *indexedmapv1.PutResponse)
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
	SetOption
	RemoveOption
	timestamp time.Timestamp
}

func (o TimestampOption) beforePut(request *indexedmapv1.PutRequest) {
	timestamp := o.timestamp.Scheme().Codec().EncodeTimestamp(o.timestamp)
	request.Timestamp = &timestamp
}

func (o TimestampOption) afterPut(response *indexedmapv1.PutResponse) {

}

func (o TimestampOption) beforeRemove(request *indexedmapv1.RemoveRequest) {
	timestamp := o.timestamp.Scheme().Codec().EncodeTimestamp(o.timestamp)
	request.Timestamp = &timestamp
}

func (o TimestampOption) afterRemove(response *indexedmapv1.RemoveResponse) {

}

// IfNotSet sets the value if the entry is not yet set
func IfNotSet() SetOption {
	return &NotSetOption{}
}

// NotSetOption is a SetOption that sets the value only if it's not already set
type NotSetOption struct {
}

func (o NotSetOption) beforePut(request *indexedmapv1.PutRequest) {
	request.Preconditions = append(request.Preconditions, indexedmapv1.Precondition{
		Precondition: &indexedmapv1.Precondition_Metadata{
			Metadata: &runtimev1.ObjectMeta{
				Type: runtimev1.ObjectMeta_TOMBSTONE,
			},
		},
	})
}

func (o NotSetOption) afterPut(response *indexedmapv1.PutResponse) {

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
	filter Filter
}

func (o filterOption) beforeWatch(request *indexedmapv1.EventsRequest) {
	if o.filter.Key != "" {
		request.Key.Key = o.filter.Key
	}
	if o.filter.Index > 0 {
		request.Key.Index = uint64(o.filter.Index)
	}
}

func (o filterOption) afterWatch(response *indexedmapv1.EventsResponse) {
}

// WithFilter returns a watch option that filters the watch events
func WithFilter(filter Filter) WatchOption {
	return filterOption{filter: filter}
}

// Filter is a watch filter configuration
type Filter struct {
	Key   string
	Index Index
}
