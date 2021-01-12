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
	api "github.com/atomix/api/go/atomix/primitive/lock"
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
	"github.com/atomix/go-client/pkg/client/meta"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestOptions(t *testing.T) {
	lockRequest := &api.LockInput{}
	assert.Nil(t, lockRequest.Timeout)
	WithTimeout(5 * time.Second).beforeLock(lockRequest)
	assert.Equal(t, 5*time.Second, *lockRequest.Timeout)

	unlockRequest := &api.UnlockInput{}
	assert.Equal(t, metaapi.RevisionNum(0), unlockRequest.Meta.Revision.Num)
	IfMatch(meta.ObjectMeta{Revision: 1}).beforeUnlock(unlockRequest)
	assert.Equal(t, metaapi.RevisionNum(1), unlockRequest.Meta.Revision.Num)

	isLockedRequest := &api.IsLockedInput{}
	assert.Equal(t, metaapi.RevisionNum(0), isLockedRequest.Meta.Revision.Num)
	IfMatch(meta.ObjectMeta{Revision: 2}).beforeIsLocked(isLockedRequest)
	assert.Equal(t, metaapi.RevisionNum(2), isLockedRequest.Meta.Revision.Num)
}
