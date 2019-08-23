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

import "fmt"

func NewName(namespace string, group string, application string, name string) Name {
	return Name{
		Namespace:   namespace,
		Group:       group,
		Application: application,
		Name:        name,
	}
}

type Name struct {
	Namespace   string
	Group       string
	Application string
	Name        string
}

func (n Name) String() string {
	return fmt.Sprintf("%s.%s.%s.%s", n.Namespace, n.Group, n.Application, n.Name)
}

// Primitive is the base interface for primitives
type Primitive interface {
	// Name returns the fully namespaced primitive name
	Name() Name

	// Close closes the primitive
	Close() error

	// Delete deletes the primitive state from the cluster
	Delete() error
}
