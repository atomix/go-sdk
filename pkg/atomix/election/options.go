// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import "github.com/google/uuid"

// Option is a counter option
type Option interface {
	apply(options *Options)
}

// Options is counter options
type Options struct {
	CandidateID string
}

func (o Options) apply(opts ...Option) {
	for _, opt := range opts {
		opt.apply(&o)
	}
	if o.CandidateID == "" {
		o.CandidateID = uuid.New().String()
	}
}

func newFuncOption(f func(*Options)) Option {
	return funcOption{f}
}

type funcOption struct {
	f func(*Options)
}

func (o funcOption) apply(options *Options) {
	o.f(options)
}

func WithCandidateID(candidateID string) Option {
	return newFuncOption(func(options *Options) {
		options.CandidateID = candidateID
	})
}
