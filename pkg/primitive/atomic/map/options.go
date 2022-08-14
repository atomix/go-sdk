// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"github.com/atomix/go-client/pkg/generic/scalar"
	"github.com/atomix/go-client/pkg/primitive/atomic"
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/map/v1"
	"time"
)

// PutOption is an option for the Put method
type PutOption interface {
	beforePut(request *mapv1.PutRequest)
	afterPut(response *mapv1.PutResponse)
}

// InsertOption is an option for the Insert method
type InsertOption interface {
	beforeInsert(request *mapv1.InsertRequest)
	afterInsert(response *mapv1.InsertResponse)
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

// WithTTL sets time-to-live for an entry
func WithTTL(ttl time.Duration) TTLOption {
	return TTLOption{ttl: ttl}
}

// TTLOption is an option for update operations setting a TTL on the map entry
type TTLOption struct {
	PutOption
	InsertOption
	UpdateOption
	ttl time.Duration
}

func (o TTLOption) beforePut(request *mapv1.PutRequest) {
	request.TTL = &o.ttl
}

func (o TTLOption) afterPut(response *mapv1.PutResponse) {

}

func (o TTLOption) beforeInsert(request *mapv1.InsertRequest) {
	request.TTL = &o.ttl
}

func (o TTLOption) afterInsert(response *mapv1.InsertResponse) {

}

func (o TTLOption) beforeUpdate(request *mapv1.UpdateRequest) {
	request.TTL = &o.ttl
}

func (o TTLOption) afterUpdate(response *mapv1.UpdateResponse) {

}

// IfVersion sets the required version for optimistic concurrency control
func IfVersion(version atomic.Version) VersionOption {
	return VersionOption{version: version}
}

// VersionOption is an implementation of PutOption and RemoveOption to specify the version for concurrency control
type VersionOption struct {
	PutOption
	UpdateOption
	RemoveOption
	version atomic.Version
}

func (o VersionOption) beforePut(request *mapv1.PutRequest) {
	request.PrevVersion = uint64(o.version)
}

func (o VersionOption) afterPut(response *mapv1.PutResponse) {

}

func (o VersionOption) beforeUpdate(request *mapv1.UpdateRequest) {
	request.PrevVersion = uint64(o.version)
}

func (o VersionOption) afterUpdate(response *mapv1.UpdateResponse) {

}

func (o VersionOption) beforeRemove(request *mapv1.RemoveRequest) {
	request.PrevVersion = uint64(o.version)
}

func (o VersionOption) afterRemove(response *mapv1.RemoveResponse) {

}

// GetOption is an option for the Get method
type GetOption interface {
	beforeGet(request *mapv1.GetRequest)
	afterGet(response *mapv1.GetResponse)
}

// EventsOption is an option for the Events method
type EventsOption interface {
	beforeEvents(request *mapv1.EventsRequest)
	afterEvents(response *mapv1.EventsResponse)
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

func (o filterOption) beforeEvents(request *mapv1.EventsRequest) {
	if o.filter.Key != "" {
		request.Key = o.filter.Key
	}
}

func (o filterOption) afterEvents(response *mapv1.EventsResponse) {
}

// WithFilter returns a watch option that filters the watch events
func WithFilter(filter Filter) EventsOption {
	return filterOption{filter: filter}
}

// Filter is a watch filter configuration
type Filter struct {
	Key string
}
