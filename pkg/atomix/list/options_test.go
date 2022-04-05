// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/list"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptions(t *testing.T) {
	request := &api.EventsRequest{}
	assert.False(t, request.Replay)
	WithReplay().beforeWatch(request)
	assert.True(t, request.Replay)
}
