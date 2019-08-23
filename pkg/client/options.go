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

package client

import "os"

func applyOptions(opts ...ClientOption) *clientOptions {
	options := &clientOptions{
		namespace: os.Getenv("ATOMIX_NAMESPACE"),
		application: os.Getenv("ATOMIX_APP"),
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	return options
}

type clientOptions struct {
	application string
	namespace   string
}

type ClientOption interface {
	apply(options *clientOptions)
}

type applicationOption struct {
	application string
}

func (o *applicationOption) apply(options *clientOptions) {
	options.application = o.application
}

func WithApplication(application string) ClientOption {
	return &applicationOption{application: application}
}

type namespaceOption struct {
	namespace string
}

func (o *namespaceOption) apply(options *clientOptions) {
	options.namespace = o.namespace
}

func WithNamespace(namespace string) ClientOption {
	return &namespaceOption{namespace: namespace}
}
