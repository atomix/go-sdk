// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package primitive

// Option is a primitive option
type Option interface {
	applyNew(*newOptions)
}

// EmptyOption is an empty primitive option
type EmptyOption struct{}

func (EmptyOption) applyNew(*newOptions) {}

// newOptions is a set of primitive options
type newOptions struct {
	clusterKey string
	sessionID  string
}

// WithClusterKey sets the primitive cluster key
func WithClusterKey(clusterKey string) Option {
	return &clusterKeyOption{
		clusterKey: clusterKey,
	}
}

// clusterKeyOption is a cluster key option
type clusterKeyOption struct {
	clusterKey string
}

func (o *clusterKeyOption) applyNew(options *newOptions) {
	options.clusterKey = o.clusterKey
}

// WithSessionID sets the primitive session identifier
func WithSessionID(sessionID string) Option {
	return &sessionIDOption{
		sessionID: sessionID,
	}
}

// sessionIDOption is a session identifier option
type sessionIDOption struct {
	sessionID string
}

func (o *sessionIDOption) applyNew(options *newOptions) {
	options.sessionID = o.sessionID
}
