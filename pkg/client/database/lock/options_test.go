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
	api "github.com/atomix/api/proto/atomix/lock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestOptions(t *testing.T) {
	lockRequest := &api.LockRequest{}
	assert.Nil(t, lockRequest.Timeout)
	WithTimeout(5 * time.Second).beforeLock(lockRequest)
	assert.Equal(t, 5*time.Second, *lockRequest.Timeout)

	unlockRequest := &api.UnlockRequest{}
	assert.Equal(t, uint64(0), unlockRequest.Version)
	IfVersion(uint64(1)).beforeUnlock(unlockRequest)
	assert.Equal(t, uint64(1), unlockRequest.Version)

	isLockedRequest := &api.IsLockedRequest{}
	assert.Equal(t, uint64(0), isLockedRequest.Version)
	IfVersion(uint64(2)).beforeIsLocked(isLockedRequest)
	assert.Equal(t, uint64(2), isLockedRequest.Version)
}
