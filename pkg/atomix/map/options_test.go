// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package _map //nolint:golint

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/map"
	"github.com/atomix/atomix-go-framework/pkg/atomix/meta"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptions(t *testing.T) {
	putRequest := &api.PutRequest{}
	IfMatch(meta.ObjectMeta{Revision: 1}).beforePut(putRequest)
	assert.Equal(t, meta.Revision(1), meta.Revision(putRequest.Preconditions[0].GetMetadata().Revision.Num))

	removeRequest := &api.RemoveRequest{}
	IfMatch(meta.ObjectMeta{Revision: 2}).beforeRemove(removeRequest)
	assert.Equal(t, meta.Revision(2), meta.Revision(removeRequest.Preconditions[0].GetMetadata().Revision.Num))

	eventRequest := &api.EventsRequest{}
	assert.False(t, eventRequest.Replay)
	WithReplay().beforeWatch(eventRequest)
	assert.True(t, eventRequest.Replay)
}
