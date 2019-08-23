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
	api "github.com/atomix/atomix-api/proto/atomix/lock"
	"time"
)

type LockOption interface {
	before(request *api.LockRequest)
	after(response *api.LockResponse)
}

func WithTimeout(timeout time.Duration) LockOption {
	return timeoutOption{timeout: timeout}
}

type timeoutOption struct {
	timeout time.Duration
}

func (o timeoutOption) before(request *api.LockRequest) {
	request.Timeout = &o.timeout
}

func (o timeoutOption) after(response *api.LockResponse) {

}

type UnlockOption interface {
	before(request *api.UnlockRequest)
	after(response *api.UnlockResponse)
}

func WithVersion(version uint64) UnlockOption {
	return versionOption{version: version}
}

type versionOption struct {
	version uint64
}

func (o versionOption) before(request *api.UnlockRequest) {
	request.Version = o.version
}

func (o versionOption) after(response *api.UnlockResponse) {

}

type IsLockedOption interface {
	before(request *api.IsLockedRequest)
	after(response *api.IsLockedResponse)
}

func WithIsVersion(version uint64) IsLockedOption {
	return isVersionOption{version: version}
}

type isVersionOption struct {
	version uint64
}

func (o isVersionOption) before(request *api.IsLockedRequest) {
	request.Version = o.version
}

func (o isVersionOption) after(response *api.IsLockedResponse) {

}
