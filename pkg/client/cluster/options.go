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

package cluster

import (
	"os"
	"time"
)

func applyOptions(opts ...Option) clusterOptions {
	options := &clusterOptions{
		namespace: os.Getenv("ATOMIX_NAMESPACE"),
		scope:     os.Getenv("ATOMIX_SCOPE"),
		peerPort:  8080,
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	return *options
}

type clusterOptions struct {
	memberID    string
	peerHost    string
	peerPort    int
	joinTimeout *time.Duration
	scope       string
	namespace   string
}

// Option provides a client option
type Option interface {
	apply(options *clusterOptions)
}

// WithMemberID configures the client's member ID
func WithMemberID(memberID string) Option {
	return &memberIDOption{id: memberID}
}

type memberIDOption struct {
	id string
}

func (o *memberIDOption) apply(options *clusterOptions) {
	options.memberID = o.id
	if options.peerHost == "" {
		options.peerHost = o.id
	}
}

// WithPeerHost configures the client's peer host
func WithPeerHost(host string) Option {
	return &peerHostOption{host: host}
}

type peerHostOption struct {
	host string
}

func (o *peerHostOption) apply(options *clusterOptions) {
	options.peerHost = o.host
}

// WithPeerPort configures the client's peer port
func WithPeerPort(port int) Option {
	return &peerPortOption{port: port}
}

type peerPortOption struct {
	port int
}

func (o *peerPortOption) apply(options *clusterOptions) {
	options.peerPort = o.port
}

// WithJoinTimeout configures the client's join timeout
func WithJoinTimeout(timeout time.Duration) Option {
	return &joinTimeoutOption{timeout: timeout}
}

type joinTimeoutOption struct {
	timeout time.Duration
}

func (o *joinTimeoutOption) apply(options *clusterOptions) {
	options.joinTimeout = &o.timeout
}

type scopeOption struct {
	scope string
}

func (o *scopeOption) apply(options *clusterOptions) {
	options.scope = o.scope
}

// WithScope configures the application scope for the client
func WithScope(scope string) Option {
	return &scopeOption{scope: scope}
}

type namespaceOption struct {
	namespace string
}

func (o *namespaceOption) apply(options *clusterOptions) {
	options.namespace = o.namespace
}

// WithNamespace configures the client's partition group namespace
func WithNamespace(namespace string) Option {
	return &namespaceOption{namespace: namespace}
}

func applyPartitionGroupOptions(opts ...PartitionGroupOption) partitionGroupOptions {
	options := &partitionGroupOptions{
		partitions:        1,
		replicationFactor: 0,
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	return *options
}

type partitionGroupOptions struct {
	partitions        int
	replicationFactor int
}

// PartitionGroupOption provides a partition group option
type PartitionGroupOption interface {
	apply(options *partitionGroupOptions)
}

// WithPartitions configures the number of partitions
func WithPartitions(partitions int) PartitionGroupOption {
	return &partitionGroupPartitionsOption{partitions: partitions}
}

type partitionGroupPartitionsOption struct {
	partitions int
}

func (o *partitionGroupPartitionsOption) apply(options *partitionGroupOptions) {
	options.partitions = o.partitions
}

// WithReplicationFactor configures the replication factor
func WithReplicationFactor(replicationFactor int) PartitionGroupOption {
	return &partitionGroupReplicationFactorOption{replicationFactor: replicationFactor}
}

type partitionGroupReplicationFactorOption struct {
	replicationFactor int
}

func (o *partitionGroupReplicationFactorOption) apply(options *partitionGroupOptions) {
	options.replicationFactor = o.replicationFactor
}
