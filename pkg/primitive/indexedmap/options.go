// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	"github.com/atomix/go-sdk/pkg/generic/scalar"
	"github.com/atomix/go-sdk/pkg/primitive"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/runtime/indexedmap/v1"
	"time"
)

// AppendOption is an option for the Append method
type AppendOption interface {
	beforeAppend(request *indexedmapv1.AppendRequest)
	afterAppend(response *indexedmapv1.AppendResponse)
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

// WithTTL sets time-to-live for an entry
func WithTTL(ttl time.Duration) TTLOption {
	return TTLOption{ttl: ttl}
}

// TTLOption is an option for update operations setting a TTL on the map entry
type TTLOption struct {
	AppendOption
	UpdateOption
	ttl time.Duration
}

func (o TTLOption) beforeAppend(request *indexedmapv1.AppendRequest) {
	request.TTL = &o.ttl
}

func (o TTLOption) afterAppend(response *indexedmapv1.AppendResponse) {

}

func (o TTLOption) beforeUpdate(request *indexedmapv1.UpdateRequest) {
	request.TTL = &o.ttl
}

func (o TTLOption) afterUpdate(response *indexedmapv1.UpdateResponse) {

}

// IfVersion sets the required version for optimistic concurrency control
func IfVersion(version primitive.Version) VersionOption {
	return VersionOption{version: version}
}

// VersionOption is an implementation of PutOption and RemoveOption to specify the version for concurrency control
type VersionOption struct {
	UpdateOption
	RemoveOption
	version primitive.Version
}

func (o VersionOption) beforeUpdate(request *indexedmapv1.UpdateRequest) {
	request.PrevVersion = uint64(o.version)
}

func (o VersionOption) afterUpdate(response *indexedmapv1.UpdateResponse) {

}

func (o VersionOption) beforeRemove(request *indexedmapv1.RemoveRequest) {
	request.PrevVersion = uint64(o.version)
}

func (o VersionOption) afterRemove(response *indexedmapv1.RemoveResponse) {

}

// GetOption is an option for the Get method
type GetOption interface {
	beforeGet(request *indexedmapv1.GetRequest)
	afterGet(response *indexedmapv1.GetResponse)
}

// EventsOption is an option for the Events method
type EventsOption interface {
	beforeEvents(request *indexedmapv1.EventsRequest)
	afterEvents(response *indexedmapv1.EventsResponse)
}

func WithKey[K scalar.Scalar](key K) EventsOption {
	return filterOption{
		filter: Filter{
			Key: scalar.NewEncodeFunc[K]()(key),
		},
	}
}

type filterOption struct {
	filter Filter
}

func (o filterOption) beforeEvents(request *indexedmapv1.EventsRequest) {
	if o.filter.Key != "" {
		request.Key = o.filter.Key
	}
}

func (o filterOption) afterEvents(response *indexedmapv1.EventsResponse) {
}

// WithFilter returns a watch option that filters the watch events
func WithFilter(filter Filter) EventsOption {
	return filterOption{filter: filter}
}

// Filter is a watch filter configuration
type Filter struct {
	Key string
}
