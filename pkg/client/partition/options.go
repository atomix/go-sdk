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

package partition

type options struct {
	partitions        int
	replicationFactor int
}

func applyOptions(opts ...Option) options {
	options := &options{
		partitions:        1,
		replicationFactor: 0,
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	return *options
}

// Option provides a partition group option
type Option interface {
	apply(options *options)
}

// WithPartitions configures the number of partitions
func WithPartitions(partitions int) Option {
	return &groupPartitionsOption{partitions: partitions}
}

type groupPartitionsOption struct {
	partitions int
}

func (o *groupPartitionsOption) apply(options *options) {
	options.partitions = o.partitions
}

// WithReplicationFactor configures the replication factor
func WithReplicationFactor(replicationFactor int) Option {
	return &groupReplicationFactorOption{replicationFactor: replicationFactor}
}

type groupReplicationFactorOption struct {
	replicationFactor int
}

func (o *groupReplicationFactorOption) apply(options *options) {
	options.replicationFactor = o.replicationFactor
}
