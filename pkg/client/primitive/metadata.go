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

// NewName returns a qualified primitive name with the given namespace, group, application, and name
func NewName(namespace string, protocol string, scope string, name string) Name {
	return Name{
		Namespace: namespace,
		Protocol:  protocol,
		Scope:     scope,
		Name:      name,
	}
}

// Name is a qualified primitive name consisting of Namespace, Database, Application, and Name
type Name struct {
	// Namespace is the namespace within which the database is stored
	Namespace string
	// Protocol is the protocol with which the primitive is stored
	Protocol string
	// Scope is the application scope in which the primitive is stored
	Scope string
	// Name is the simple name of the primitive
	Name string
}

func (n Name) String() string {
	return fmt.Sprintf("%s.%s.%s.%s", n.Namespace, n.Protocol, n.Scope, n.Name)
}

// Type is the type of a primitive
type Type string

// Metadata provides primitive metadata
type Metadata struct {
	// Type is the primitive type
	Type Type
	// Name is the primitive name
	Name Name
}
