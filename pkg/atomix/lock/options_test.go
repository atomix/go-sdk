// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	metaapi "github.com/atomix/api/pkg/atomix/atom/meta/v1"
	api "github.com/atomix/api/pkg/atomix/lock/v1"
	"github.com/atomix/runtime/pkg/meta"
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
