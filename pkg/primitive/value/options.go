// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package value

import (
	"github.com/atomix/go-client/pkg/primitive"
	valuev1 "github.com/atomix/runtime/api/atomix/runtime/value/v1"
	"time"
)

// SetOption is an option for the Put method
type SetOption interface {
	beforeSet(request *valuev1.SetRequest)
	afterSet(response *valuev1.SetResponse)
}

// UpdateOption is an option for the Update method
type UpdateOption interface {
	beforeUpdate(request *valuev1.UpdateRequest)
	afterUpdate(response *valuev1.UpdateResponse)
}

// DeleteOption is an option for the Delete method
type DeleteOption interface {
	beforeDelete(request *valuev1.DeleteRequest)
	afterDelete(response *valuev1.DeleteResponse)
}

// WithTTL sets time-to-live for an entry
func WithTTL(ttl time.Duration) TTLOption {
	return TTLOption{ttl: ttl}
}

// TTLOption is an option for update operations setting a TTL on the map entry
type TTLOption struct {
	SetOption
	UpdateOption
	ttl time.Duration
}

func (o TTLOption) beforeSet(request *valuev1.SetRequest) {
	request.TTL = &o.ttl
}

func (o TTLOption) afterSet(response *valuev1.SetResponse) {

}

func (o TTLOption) beforeUpdate(request *valuev1.UpdateRequest) {
	request.TTL = &o.ttl
}

func (o TTLOption) afterUpdate(response *valuev1.UpdateResponse) {

}

// IfVersion sets the required version for optimistic concurrency control
func IfVersion(version primitive.Version) VersionOption {
	return VersionOption{version: version}
}

// VersionOption is an implementation of SetOption and DeleteOption to specify the version for concurrency control
type VersionOption struct {
	UpdateOption
	DeleteOption
	version primitive.Version
}

func (o VersionOption) beforeUpdate(request *valuev1.UpdateRequest) {
	request.PrevVersion = uint64(o.version)
}

func (o VersionOption) afterUpdate(response *valuev1.UpdateResponse) {

}

func (o VersionOption) beforeDelete(request *valuev1.DeleteRequest) {
	request.PrevVersion = uint64(o.version)
}

func (o VersionOption) afterDelete(response *valuev1.DeleteResponse) {

}

// GetOption is an option for the Get method
type GetOption interface {
	beforeGet(request *valuev1.GetRequest)
	afterGet(response *valuev1.GetResponse)
}

// EventsOption is an option for the Events method
type EventsOption interface {
	beforeEvents(request *valuev1.EventsRequest)
	afterEvents(response *valuev1.EventsResponse)
}
