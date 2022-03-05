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
