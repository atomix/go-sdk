// Copyright 2020-present Open Networking Foundation.
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

package atomix

func newOptions(opts ...Option) Options {
	options := Options{
		Host: defaultHost,
		Port: defaultPort,
	}
	options.apply(opts...)
	return options
}

// Options is a set of options for configuring the Atomix client
type Options struct {
	Host string
	Port int
}

func (o *Options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

// Option is an option for configuring the Atomix client
type Option func(*Options)

// WithHost overrides the default host
func WithHost(host string) Option {
	return func(options *Options) {
		options.Host = host
	}
}

// WithPort overrides the default port
func WithPort(port int) Option {
	return func(options *Options) {
		options.Port = port
	}
}
