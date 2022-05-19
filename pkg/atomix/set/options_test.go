// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package set

import (
	api "github.com/atomix/api/pkg/atomix/set/v1"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptions(t *testing.T) {
	request := &api.EventsRequest{}
	assert.False(t, request.Replay)
	WithReplay().beforeWatch(request)
	assert.True(t, request.Replay)
}
