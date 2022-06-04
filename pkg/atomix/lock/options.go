// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	rttime "github.com/atomix/runtime/pkg/atomix/time"
	"time"
)

// Option is a counter option
type Option interface {
	apply(options *Options)
}

// Options is counter options
type Options struct{}

func (o Options) apply(opts ...Option) {
	for _, opt := range opts {
		opt.apply(&o)
	}
}

func newFuncOption(f func(*Options)) Option {
	return funcOption{f}
}

type funcOption struct {
	f func(*Options)
}

func (o funcOption) apply(options *Options) {
	o.f(options)
}

// LockOption is an option for Lock calls
//nolint:golint
type LockOption interface {
	beforeLock(request *lockv1.LockRequest)
	afterLock(response *lockv1.LockResponse)
}

// WithTimeout sets the lock timeout
func WithTimeout(timeout time.Duration) LockOption {
	return timeoutOption{timeout: timeout}
}

type timeoutOption struct {
	timeout time.Duration
}

func (o timeoutOption) beforeLock(request *lockv1.LockRequest) {
	request.Timeout = &o.timeout
}

func (o timeoutOption) afterLock(response *lockv1.LockResponse) {

}

// UnlockOption is an option for Unlock calls
type UnlockOption interface {
	beforeUnlock(request *lockv1.UnlockRequest)
	afterUnlock(response *lockv1.UnlockResponse)
}

// GetOption is an option for IsLocked calls
type GetOption interface {
	beforeGet(request *lockv1.GetLockRequest)
	afterGet(response *lockv1.GetLockResponse)
}

// IfTimestamp sets the lock timestamp to check
func IfTimestamp(timestamp rttime.Timestamp) TimestampOption {
	return TimestampOption{timestamp: timestamp}
}

// TimestampOption is a lock option for checking the version
type TimestampOption struct {
	timestamp rttime.Timestamp
}

func (o TimestampOption) beforeUnlock(request *lockv1.UnlockRequest) {
	timestamp := o.timestamp.Scheme().Codec().EncodeTimestamp(o.timestamp)
	request.Lock.Timestamp = &timestamp
}

func (o TimestampOption) afterUnlock(response *lockv1.UnlockResponse) {

}

func (o TimestampOption) beforeGet(request *lockv1.GetLockRequest) {
	timestamp := o.timestamp.Scheme().Codec().EncodeTimestamp(o.timestamp)
	request.Lock.Timestamp = &timestamp
}

func (o TimestampOption) afterGet(response *lockv1.GetLockResponse) {

}
