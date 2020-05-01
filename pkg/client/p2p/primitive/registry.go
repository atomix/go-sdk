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

import "google.golang.org/grpc"

var registry = &serviceRegistry{services: []Service{}}

// Service is a peer-to-peer primitive service
type Service func(server *grpc.Server)

// RegisterService registers a peer-to-peer primitive service
func RegisterService(service Service) {
	registry.register(service)
}

// serviceRegistry is a peer-to-peer primitive service registry
type serviceRegistry struct {
	services []Service
}

func (r *serviceRegistry) register(service Service) {
	r.services = append(r.services, service)
}
