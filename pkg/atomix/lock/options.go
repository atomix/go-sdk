// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	"github.com/atomix/runtime/pkg/meta"
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

type funcOption[K, V any] struct {
	f func(*Options)
}

func (o funcOption) apply(options *Options) {
	o.f(options)
}

// newLockOptions is lock options
type newLockOptions struct{}

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

// IfMatch sets the lock version to check
func IfMatch(object meta.Object) MatchOption {
	return MatchOption{object: object}
}

// MatchOption is a lock option for checking the version
type MatchOption struct {
	object meta.Object
}

func (o MatchOption) beforeUnlock(request *lockv1.UnlockRequest) {
	request.Lock.ObjectMeta = o.object.Meta().Proto()
}

func (o MatchOption) afterUnlock(response *lockv1.UnlockResponse) {

}

func (o MatchOption) beforeGet(request *lockv1.GetLockRequest) {
	request.Lock.ObjectMeta = o.object.Meta().Proto()
}

func (o MatchOption) afterGet(response *lockv1.GetLockResponse) {

}
