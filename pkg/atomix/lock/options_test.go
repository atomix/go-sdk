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
	metaapi "github.com/atomix/atomix-api/go/atomix/primitive/meta"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
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
	IfMatch(meta.ObjectMeta{Revision: 1}).beforeUnlock(unlockRequest)
	assert.Equal(t, metaapi.RevisionNum(1), unlockRequest.Lock.ObjectMeta.Revision.Num)

	getLockRequest := &api.GetLockRequest{}
	IfMatch(meta.ObjectMeta{Revision: 2}).beforeGet(getLockRequest)
	assert.Equal(t, metaapi.RevisionNum(2), getLockRequest.Lock.ObjectMeta.Revision.Num)
}
