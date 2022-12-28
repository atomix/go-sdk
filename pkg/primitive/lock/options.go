// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	"time"
)

// LockOption is an option for Lock calls
//
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
