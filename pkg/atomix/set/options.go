// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/set"
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"github.com/atomix/go-client/pkg/atomix/primitive/codec"
)

// Option is a set option
type Option[E any] interface {
	primitive.Option
	applyNewSet(options *newSetOptions[E])
}

// newSetOptions is set options
type newSetOptions[E any] struct {
	elementCodec codec.Codec[E]
}

func WithCodec[E any](elementCodec codec.Codec[E]) Option[E] {
	return codecOption[E]{
		elementCodec: elementCodec,
	}
}

type codecOption[E any] struct {
	primitive.EmptyOption
	elementCodec codec.Codec[E]
}

func (o codecOption[E]) applyNewSet(options *newSetOptions[E]) {
	options.elementCodec = o.elementCodec
}

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
