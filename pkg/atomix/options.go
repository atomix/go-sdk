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

// Option is a client option
type Option interface {
	apply(*clientOptions)
}

// EmptyOption is an empty primitive option
type EmptyOption struct{}

func (EmptyOption) apply(*clientOptions) {}

// clientOptions is a set of client options
type clientOptions struct {
	clientID   string
	brokerHost string
	brokerPort int
}

// WithClientID sets the client identifier
func WithClientID(clientID string) Option {
	return &clientIDOption{
		clientID: clientID,
	}
}

// clientIDOption is a broker host option
type clientIDOption struct {
	clientID string
}

func (o *clientIDOption) apply(options *clientOptions) {
	options.clientID = o.clientID
}

// WithBrokerHost sets the broker host
func WithBrokerHost(host string) Option {
	return &hostOption{
		host: host,
	}
}

// hostOption is a broker host option
type hostOption struct {
	host string
}

func (o *hostOption) apply(options *clientOptions) {
	options.brokerHost = o.host
}

// WithBrokerPort sets the broker port
func WithBrokerPort(port int) Option {
	return &portOption{
		port: port,
	}
}

// portOption is a broker port option
type portOption struct {
	port int
}

func (o *portOption) apply(options *clientOptions) {
	options.brokerPort = o.port
}
