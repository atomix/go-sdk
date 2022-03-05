// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/indexedmap"
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive/codec"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
)

// Option is a indexed map option
type Option[K, V any] interface {
	primitive.Option
	applyNewIndexedMap(options *newIndexedMapOptions[K, V])
}

// newIndexedMapOptions is indexed map options
type newIndexedMapOptions[K, V any] struct {
	keyCodec   codec.Codec[K]
	valueCodec codec.Codec[V]
}

func WithCodec[K, V any](keyCodec codec.Codec[K], valueCodec codec.Codec[V]) Option[K, V] {
	return codecOption[K, V]{
		keyCodec:   keyCodec,
		valueCodec: valueCodec,
	}
}

type codecOption[K, V any] struct {
	primitive.EmptyOption
	keyCodec   codec.Codec[K]
	valueCodec codec.Codec[V]
}

func (o codecOption[K, V]) applyNewIndexedMap(options *newIndexedMapOptions[K, V]) {
	options.keyCodec = o.keyCodec
	options.valueCodec = o.valueCodec
}

// SetOption is an option for the Put method
type SetOption interface {
	beforePut(request *api.PutRequest)
	afterPut(response *api.PutResponse)
}

// RemoveOption is an option for the Remove method
type RemoveOption interface {
	beforeRemove(request *api.RemoveRequest)
	afterRemove(response *api.RemoveResponse)
}

// IfMatch sets the required version for optimistic concurrency control
func IfMatch(object meta.Object) MatchOption {
	return MatchOption{object: object}
}

// MatchOption is an implementation of SetOption and RemoveOption to specify the version for concurrency control
type MatchOption struct {
	SetOption
	RemoveOption
	object meta.Object
}

func (o MatchOption) beforePut(request *api.PutRequest) {
	proto := o.object.Meta().Proto()
	request.Preconditions = append(request.Preconditions, api.Precondition{
		Precondition: &api.Precondition_Metadata{
			Metadata: &proto,
		},
	})
}

func (o MatchOption) afterPut(response *api.PutResponse) {

}

func (o MatchOption) beforeRemove(request *api.RemoveRequest) {
	proto := o.object.Meta().Proto()
	request.Preconditions = append(request.Preconditions, api.Precondition{
		Precondition: &api.Precondition_Metadata{
			Metadata: &proto,
		},
	})
}

func (o MatchOption) afterRemove(response *api.RemoveResponse) {

}

// IfNotSet sets the value if the entry is not yet set
func IfNotSet() SetOption {
	return &NotSetOption{}
}

// NotSetOption is a SetOption that sets the value only if it's not already set
type NotSetOption struct {
}

func (o NotSetOption) beforePut(request *api.PutRequest) {
	request.Preconditions = append(request.Preconditions, api.Precondition{
		Precondition: &api.Precondition_Metadata{
			Metadata: &metaapi.ObjectMeta{
				Type: metaapi.ObjectMeta_TOMBSTONE,
			},
		},
	})
}

func (o NotSetOption) afterPut(response *api.PutResponse) {

}

// GetOption is an option for the Get method
type GetOption interface {
	beforeGet(request *api.GetRequest)
	afterGet(response *api.GetResponse)
}

// WatchOption is an option for the Watch method
type WatchOption interface {
	beforeWatch(request *api.EventsRequest)
	afterWatch(response *api.EventsResponse)
}

// WithReplay returns a watch option that enables replay of watch events
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *api.EventsRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *api.EventsResponse) {

}

type filterOption struct {
	filter Filter
}

func (o filterOption) beforeWatch(request *api.EventsRequest) {
	if o.filter.Key != "" {
		request.Pos.Key = o.filter.Key
	}
	if o.filter.Index > 0 {
		request.Pos.Index = uint64(o.filter.Index)
	}
}

func (o filterOption) afterWatch(response *api.EventsResponse) {
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
