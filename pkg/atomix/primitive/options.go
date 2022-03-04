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
