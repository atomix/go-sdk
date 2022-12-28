// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	"time"
)

// AddOption is an option for the Add method
type AddOption interface {
	beforeAdd(request *setv1.AddRequest)
	afterAdd(response *setv1.AddResponse)
}

// RemoveOption is an option for the Remove method
type RemoveOption interface {
	beforeRemove(request *setv1.RemoveRequest)
	afterRemove(response *setv1.RemoveResponse)
}

// WithTTL sets time-to-live for an entry
func WithTTL(ttl time.Duration) TTLOption {
	return TTLOption{ttl: ttl}
}

// TTLOption is an option for update operations setting a TTL on the map entry
type TTLOption struct {
	AddOption
	ttl time.Duration
}

func (o TTLOption) beforeAdd(request *setv1.AddRequest) {
	request.TTL = &o.ttl
}

func (o TTLOption) afterAdd(response *setv1.AddResponse) {

}
