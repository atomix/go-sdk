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

package log

import (
	"testing"

	api "github.com/atomix/api/proto/atomix/log"
	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	getResponse := &api.GetResponse{}
	assert.Nil(t, getResponse.Value)
	WithDefault([]byte("foo")).afterGet(getResponse)
	assert.Equal(t, "foo", string(getResponse.Value))

	eventRequest := &api.EventRequest{}
	assert.False(t, eventRequest.Replay)
	WithReplay().beforeWatch(eventRequest)
	assert.True(t, eventRequest.Replay)
}
