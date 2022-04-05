// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/set"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
)

// Option is a set option
type Option interface {
	primitive.Option
	applyNewSet(options *newSetOptions)
}

// newSetOptions is set options
type newSetOptions struct{}

// WatchOption is an option for set Watch calls
type WatchOption interface {
	beforeWatch(request *api.EventsRequest)
	afterWatch(response *api.EventsResponse)
}

// WithReplay returns a Watch option to replay entries
func WithReplay() WatchOption {
	return replayOption{}
}

type replayOption struct{}

func (o replayOption) beforeWatch(request *api.EventsRequest) {
	request.Replay = true
}

func (o replayOption) afterWatch(response *api.EventsResponse) {

}
