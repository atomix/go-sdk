// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"os"
	"strconv"
)

const (
	runtimeHostEnv = "ATOMIX_RUNTIME_HOST"
	runtimePortEnv = "ATOMIX_RUNTIME_PORT"
)

const (
	defaultRuntimeHost = "127.0.0.1"
	defaultRuntimePort = 5678
)

// Options is a set of client options
type Options struct {
	Host string
	Port int
}

func (o Options) apply(opts ...Option) {
	for _, opt := range opts {
		opt.apply(&o)
	}

	if o.Host == "" {
		runtimeHost := os.Getenv(runtimeHostEnv)
		if runtimeHost != "" {
			o.Host = runtimeHost
		} else {
			o.Host = defaultRuntimeHost
		}
	}

	if o.Port == 0 {
		runtimePorts := os.Getenv(runtimePortEnv)
		if runtimePorts != "" {
			runtimePort, err := strconv.Atoi(runtimePorts)
			if err != nil {
				panic(err)
			}
			o.Port = runtimePort
		} else {
			o.Port = defaultRuntimePort
		}
	}
}

// Option is a client option
type Option interface {
	apply(*Options)
}

// EmptyOption is an empty primitive option
type EmptyOption struct{}

func (EmptyOption) apply(*Options) {}

func newFuncOption(f func(*Options)) Option {
	return &funcOption{f}
}

type funcOption struct {
	f func(*Options)
}

func (o *funcOption) apply(options *Options) {
	o.f(options)
}

// WithOptions sets the client options
func WithOptions(opts Options) Option {
	return newFuncOption(func(options *Options) {
		*options = opts
	})
}

// WithHost sets the runtime host
func WithHost(host string) Option {
	return newFuncOption(func(options *Options) {
		options.Host = host
	})
}

// WithPort sets the runtime port
func WithPort(port int) Option {
	return newFuncOption(func(options *Options) {
		options.Port = port
	})
}
