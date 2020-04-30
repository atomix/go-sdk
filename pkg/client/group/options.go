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

package group

import (
	"os"
	"time"
)

func applyOptions(opts ...MembershipGroupOption) groupOptions {
	options := &groupOptions{
		namespace: os.Getenv("ATOMIX_NAMESPACE"),
		scope:     os.Getenv("ATOMIX_SCOPE"),
	}
	for _, opt := range opts {
		opt.applyGroup(options)
	}
	return *options
}

type groupOptions struct {
	memberID    string
	joinTimeout *time.Duration
	scope       string
	namespace   string
}

// Option provides a group option
type Option interface {
	MembershipGroupOption
	PartitionGroupOption
}

// MembershipGroupOption provides a membership group option
type MembershipGroupOption interface {
	applyGroup(options *groupOptions)
}

// WithMemberID configures the client's member ID
func WithMemberID(memberID string) Option {
	return &memberIDOption{id: memberID}
}

type memberIDOption struct {
	id string
}

func (o *memberIDOption) applyGroup(options *groupOptions) {
	options.memberID = o.id
}

func (o *memberIDOption) applyPartitionGroup(options *partitionGroupOptions) {
	options.memberID = o.id
}

// WithJoinTimeout configures the client's join timeout
func WithJoinTimeout(timeout time.Duration) Option {
	return &joinTimeoutOption{timeout: timeout}
}

type joinTimeoutOption struct {
	timeout time.Duration
}

func (o *joinTimeoutOption) applyGroup(options *groupOptions) {
	options.joinTimeout = &o.timeout
}

func (o *joinTimeoutOption) applyPartitionGroup(options *partitionGroupOptions) {
	options.joinTimeout = &o.timeout
}

// WithScope configures the application scope for the client
func WithScope(scope string) Option {
	return &scopeOption{scope: scope}
}

type scopeOption struct {
	scope string
}

func (o *scopeOption) applyGroup(options *groupOptions) {
	options.scope = o.scope
}

func (o *scopeOption) applyPartitionGroup(options *partitionGroupOptions) {
	options.scope = o.scope
}

// WithNamespace configures the client's partition group namespace
func WithNamespace(namespace string) Option {
	return &namespaceOption{namespace: namespace}
}

type namespaceOption struct {
	namespace string
}

func (o *namespaceOption) applyGroup(options *groupOptions) {
	options.namespace = o.namespace
}

func (o *namespaceOption) applyPartitionGroup(options *partitionGroupOptions) {
	options.namespace = o.namespace
}

type partitionGroupOptions struct {
	memberID          string
	joinTimeout       *time.Duration
	scope             string
	namespace         string
	partitions        int
	replicationFactor int
}

func applyPartitionGroupOptions(opts ...PartitionGroupOption) partitionGroupOptions {
	options := &partitionGroupOptions{
		namespace:         os.Getenv("ATOMIX_NAMESPACE"),
		scope:             os.Getenv("ATOMIX_SCOPE"),
		partitions:        1,
		replicationFactor: 0,
	}
	for _, opt := range opts {
		opt.applyPartitionGroup(options)
	}
	return *options
}

// PartitionGroupOption provides a partition group option
type PartitionGroupOption interface {
	applyPartitionGroup(options *partitionGroupOptions)
}

// WithPartitions configures the number of partitions
func WithPartitions(partitions int) PartitionGroupOption {
	return &partitionGroupPartitionsOption{partitions: partitions}
}

type partitionGroupPartitionsOption struct {
	partitions int
}

func (o *partitionGroupPartitionsOption) applyPartitionGroup(options *partitionGroupOptions) {
	options.partitions = o.partitions
}

// WithReplicationFactor configures the replication factor
func WithReplicationFactor(replicationFactor int) PartitionGroupOption {
	return &partitionGroupReplicationFactorOption{replicationFactor: replicationFactor}
}

type partitionGroupReplicationFactorOption struct {
	replicationFactor int
}

func (o *partitionGroupReplicationFactorOption) applyPartitionGroup(options *partitionGroupOptions) {
	options.replicationFactor = o.replicationFactor
}
