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

package primitive

import (
	"context"
)

// Type is the type of a primitive
type Type string

// Primitive is the base interface for primitives
type Primitive interface {
	// Type returns the primitive type
	Type() Type

	// Name returns the primitive name
	Name() string

	// Close closes the primitive
	Close(ctx context.Context) error

	// Delete deletes the primitive state from the cluster
	Delete(ctx context.Context) error
}
