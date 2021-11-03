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

package indexedmap

import (
	api "github.com/atomix/atomix-api/go/atomix/primitive/indexedmap/v1"
	"github.com/atomix/atomix-sdk-go/pkg/meta"
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
