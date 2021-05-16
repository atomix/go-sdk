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
