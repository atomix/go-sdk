// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package _map //nolint:golint

import (
	times "github.com/atomix/go-client/pkg/client/time"
	"time"
)

func applyGossipMapOptions(opts ...GossipMapOption) gossipMapOptions {
	options := &gossipMapOptions{
		gossipPeriod:      50 * time.Millisecond,
		antiEntropyPeriod: time.Second,
		clock:             times.NewLogicalClock(),
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	return *options
}

// GossipMapOption is an option for a gossip Map instance
type GossipMapOption interface {
	apply(options *gossipMapOptions)
}

// gossipMapOptions is a set of gossip map options
type gossipMapOptions struct {
	clock             times.Clock
	gossipPeriod      time.Duration
	antiEntropyPeriod time.Duration
}

// WithClock sets the gossip clock
func WithClock(clock times.Clock) GossipMapOption {
	return &gossipClockOption{
		clock: clock,
	}
}

type gossipClockOption struct {
	clock times.Clock
}

func (o *gossipClockOption) apply(options *gossipMapOptions) {
	options.clock = o.clock
}

// WithGossipPeriod sets the gossip period for a gossip map
func WithGossipPeriod(period time.Duration) GossipMapOption {
	return &gossipPeriodOption{
		period: period,
	}
}

type gossipPeriodOption struct {
	period time.Duration
}

func (o *gossipPeriodOption) apply(options *gossipMapOptions) {
	options.gossipPeriod = o.period
}

// WithAntiEntropyPeriod sets the anti-entropy period for a gossip map
func WithAntiEntropyPeriod(period time.Duration) GossipMapOption {
	return &antiEntropyPeriodOption{
		period: period,
	}
}

type antiEntropyPeriodOption struct {
	period time.Duration
}

func (o *antiEntropyPeriodOption) apply(options *gossipMapOptions) {
	options.antiEntropyPeriod = o.period
}

// PutOption is an option for the Put method
type PutOption interface{}

// RemoveOption is an option for the Remove method
type RemoveOption interface{}

// GetOption is an option for the Get method
type GetOption interface{}

// WatchOption is an option for the Watch method
type WatchOption interface{}
