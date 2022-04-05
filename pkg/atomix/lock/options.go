// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/lock"
	"github.com/atomix/atomix-go-client/pkg/atomix/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"time"
)

// Option is a lock option
type Option interface {
	primitive.Option
	applyNewLock(options *newLockOptions)
}

// newLockOptions is lock options
type newLockOptions struct{}

// LockOption is an option for Lock calls
//nolint:golint
type LockOption interface {
	beforeLock(request *api.LockRequest)
	afterLock(response *api.LockResponse)
}

// WithTimeout sets the lock timeout
func WithTimeout(timeout time.Duration) LockOption {
	return timeoutOption{timeout: timeout}
}

type timeoutOption struct {
	timeout time.Duration
}

func (o timeoutOption) beforeLock(request *api.LockRequest) {
	request.Timeout = &o.timeout
}

func (o timeoutOption) afterLock(response *api.LockResponse) {

}

// UnlockOption is an option for Unlock calls
type UnlockOption interface {
	beforeUnlock(request *api.UnlockRequest)
	afterUnlock(response *api.UnlockResponse)
}

// GetOption is an option for IsLocked calls
type GetOption interface {
	beforeGet(request *api.GetLockRequest)
	afterGet(response *api.GetLockResponse)
}

// IfMatch sets the lock version to check
func IfMatch(object meta.Object) MatchOption {
	return MatchOption{object: object}
}

// MatchOption is a lock option for checking the version
type MatchOption struct {
	object meta.Object
}

func (o MatchOption) beforeUnlock(request *api.UnlockRequest) {
	request.Lock.ObjectMeta = o.object.Meta().Proto()
}

func (o MatchOption) afterUnlock(response *api.UnlockResponse) {

}

func (o MatchOption) beforeGet(request *api.GetLockRequest) {
	request.Lock.ObjectMeta = o.object.Meta().Proto()
}

func (o MatchOption) afterGet(response *api.GetLockResponse) {

}
