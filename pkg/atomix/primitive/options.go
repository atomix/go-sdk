// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

type Option interface {
	apply(*Options)
}

type Options struct {
	Labels map[string]string
}

func (o Options) apply(opts ...Option) {
	for _, opt := range opts {
		opt.apply(&o)
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

func WithLabel(key, value string) Option {
	return newFuncOption(func(options *Options) {
		if options.Labels == nil {
			options.Labels = make(map[string]string)
		}
		options.Labels[key] = value
	})
}

func WithLabels(labels ...string) Option {
	return newFuncOption(func(options *Options) {
		if options.Labels == nil {
			options.Labels = make(map[string]string)
		}
		if len(labels)%2 != 0 {
			panic("expected an even number of key=value pairs")
		}
		for i := 0; i < len(labels)-1; i += 2 {
			key, value := labels[i], labels[i+1]
			options.Labels[key] = value
		}
	})
}
