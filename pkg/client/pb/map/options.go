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

package _map //nolint:golint

import (
	"time"
)

func applyOptions(opts ...Option) options {
	options := &options{
		maxBackupQueue: 1,
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	return *options
}

// Option is an option for a gossip Map instance
type Option interface {
	apply(options *options)
}

// options is a set of gossip map options
type options struct {
	backupPeriod   time.Duration
	maxBackupQueue int
}

// WithMaxBackupQueue sets the maximum backup queue size
func WithMaxBackupQueue(size int) Option {
	return &maxBackupQueueOption{
		size: size,
	}
}

type maxBackupQueueOption struct {
	size int
}

func (o *maxBackupQueueOption) apply(options *options) {
	options.maxBackupQueue = o.size
}

// WithBackupPeriod sets the backup period for a map
func WithBackupPeriod(period time.Duration) Option {
	return &backupPeriodOption{
		period: period,
	}
}

type backupPeriodOption struct {
	period time.Duration
}

func (o *backupPeriodOption) apply(options *options) {
	options.backupPeriod = o.period
}

func applyPutOptions(opts ...PutOption) putOptions {
	options := &putOptions{}
	for _, opt := range opts {
		opt.applyPut(options)
	}
	return *options
}

type putOptions struct {
	version Version
}

// PutOption is an option for the Put method
type PutOption interface {
	applyPut(options *putOptions)
}

// UpdateOption is an option for map updates
type UpdateOption interface {
	PutOption
	RemoveOption
}

// IfVersion returns a condition on the current map entry version
func IfVersion(version Version) UpdateOption {
	return &ifVersionOption{
		version: version,
	}
}

type ifVersionOption struct {
	version Version
}

func (o *ifVersionOption) applyPut(options *putOptions) {
	options.version = o.version
}

func (o *ifVersionOption) applyRemove(options *removeOptions) {
	options.version = o.version
}

func applyRemoveOptions(opts ...RemoveOption) removeOptions {
	options := &removeOptions{}
	for _, opt := range opts {
		opt.applyRemove(options)
	}
	return *options
}

type removeOptions struct {
	version Version
}

// RemoveOption is an option for the Remove method
type RemoveOption interface {
	applyRemove(options *removeOptions)
}

// GetOption is an option for the Get method
type GetOption interface{}

// WatchOption is an option for the Watch method
type WatchOption interface{}
