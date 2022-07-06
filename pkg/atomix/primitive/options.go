// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

type Options struct {
	Tags map[string]string
}

func (o Options) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(&o)
	}
}

type Option func(*Options)

func WithTags(tags map[string]string) Option {
	return func(options *Options) {
		options.Tags = tags
	}
}

func WithTag(key, value string) Option {
	return func(options *Options) {
		if options.Tags == nil {
			options.Tags = make(map[string]string)
		}
		options.Tags[key] = value
	}
}
