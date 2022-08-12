// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"github.com/atomix/go-client/pkg/atomix/primitive"
	"github.com/google/uuid"
)

// Option is a counter option
type Option interface {
	apply(options *Options)
}

// Options is counter options
type Options struct {
	primitive.Options
	CandidateID string
}

func (o *Options) Apply(opts ...Option) {
	for _, opt := range opts {
		opt.apply(o)
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

func WithTags(tags map[string]string) Option {
	return newFuncOption(func(options *Options) {
		primitive.WithTags(tags)(&options.Options)
	})
}

func WithTag(key, value string) Option {
	return newFuncOption(func(options *Options) {
		primitive.WithTag(key, value)(&options.Options)
	})
}

func WithCandidateID(candidateID string) Option {
	return newFuncOption(func(options *Options) {
		options.CandidateID = candidateID
	})
}
