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

package client

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOptions(t *testing.T) {
	_ = os.Setenv("ATOMIX_NAMESPACE", "default")
	_ = os.Setenv("ATOMIX_SCOPE", "test")
	options := applyOptions()
	assert.Equal(t, "default", options.namespace)
	assert.Equal(t, "test", options.scope)
	options = applyOptions(WithNamespace("foo"), WithScope("bar"))
	assert.Equal(t, "foo", options.namespace)
	assert.Equal(t, "bar", options.scope)
}
