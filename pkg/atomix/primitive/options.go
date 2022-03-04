// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package primitive

// Option is a primitive option
type Option[T Primitive] interface {
	applyNew(*newOptions[T])
}

// EmptyOption is an empty primitive option
type EmptyOption[T Primitive] struct{}

func (EmptyOption[T]) applyNew(*newOptions[T]) {}

// newOptions is a set of primitive options
type newOptions[T Primitive] struct {
	clusterKey string
	sessionID  string
}

// WithClusterKey sets the primitive cluster key
func WithClusterKey[T Primitive](clusterKey string) Option[T] {
	return &clusterKeyOption[T]{
		clusterKey: clusterKey,
	}
}

// clusterKeyOption is a cluster key option
type clusterKeyOption[T Primitive] struct {
	clusterKey string
}

func (o *clusterKeyOption[T]) applyNew(options *newOptions[T]) {
	options.clusterKey = o.clusterKey
}

// WithSessionID sets the primitive session identifier
func WithSessionID[T Primitive](sessionID string) Option[T] {
	return &sessionIDOption[T]{
		sessionID: sessionID,
	}
}

// sessionIDOption is a session identifier option
type sessionIDOption[T Primitive] struct {
	sessionID string
}

func (o *sessionIDOption[T]) applyNew(options *newOptions[T]) {
	options.sessionID = o.sessionID
}
