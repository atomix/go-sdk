// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package indexedmap

import (
	api "github.com/atomix/api/pkg/atomix/indexed_map/v1"
	"github.com/atomix/runtime/pkg/atomix/meta"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptions(t *testing.T) {
	putRequest := &api.PutRequest{}
	IfTimestamp(meta.ObjectMeta{Revision: 1}).beforePut(putRequest)
	assert.Equal(t, meta.Revision(1), meta.Revision(putRequest.Preconditions[0].GetMetadata().Revision.Num))

	removeRequest := &api.RemoveRequest{}
	IfTimestamp(meta.ObjectMeta{Revision: 2}).beforeRemove(removeRequest)
	assert.Equal(t, meta.Revision(2), meta.Revision(removeRequest.Preconditions[0].GetMetadata().Revision.Num))

	eventRequest := &api.EventsRequest{}
	assert.False(t, eventRequest.Replay)
	WithReplay().beforeWatch(eventRequest)
	assert.True(t, eventRequest.Replay)
}
