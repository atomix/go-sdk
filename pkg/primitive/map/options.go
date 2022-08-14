// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	"github.com/atomix/go-client/pkg/generic/scalar"
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	"time"
)

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

// WithTTL sets time-to-live for an entry
func WithTTL(ttl time.Duration) TTLOption {
	return TTLOption{ttl: ttl}
}

// TTLOption is an option for update operations setting a TTL on the map entry
type TTLOption struct {
	PutOption
	ttl time.Duration
}

func (o TTLOption) beforePut(request *mapv1.PutRequest) {
	request.Value.TTL = &o.ttl
}

func (o TTLOption) afterPut(response *mapv1.PutResponse) {

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
