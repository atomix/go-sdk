// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

// Option is a counter option
type Option interface {
	apply(options *Options)
}

// Options is counter options
type Options struct{}

func (o Options) apply(opts ...Option) {
	for _, opt := range opts {
		opt.apply(&o)
	}
}

func newFuncOption(f func(*Options)) Option {
	return funcOption{f}
}

type funcOption[K, V any] struct {
	f func(*Options)
}

func (o funcOption) apply(options *Options) {
	o.f(options)
}
