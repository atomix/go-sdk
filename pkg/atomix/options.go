// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
