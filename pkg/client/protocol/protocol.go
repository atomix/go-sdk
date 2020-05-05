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

package protocol

import (
	"context"
	primitiveapi "github.com/atomix/api/proto/atomix/primitive"
	"github.com/atomix/api/proto/atomix/protocol"
	"github.com/atomix/go-client/pkg/client/primitive"
	"google.golang.org/grpc"
)

// Protocol is an interface for protocol clients
type Protocol interface {
	Close(ctx context.Context) error
}

// New creates a new Client instance
func New(conn *grpc.ClientConn, namespace string, name string, scope string) *Client {
	return &Client{
		conn:      conn,
		Namespace: namespace,
		Name:      name,
		Scope:     scope,
	}
}

// Client is the base client for protocols
type Client struct {
	Protocol
	conn      *grpc.ClientConn
	Namespace string
	Name      string
	Scope     string
}

// GetPrimitives gets a list of primitives in the database
func (p *Client) GetPrimitives(ctx context.Context, opts ...PrimitiveOption) ([]primitive.Metadata, error) {
	options := &primitiveOptions{}
	for _, opt := range opts {
		opt.apply(options)
	}

	var primitiveType primitiveapi.PrimitiveType
	switch options.primitiveType {
	case "Counter":
		primitiveType = primitiveapi.PrimitiveType_COUNTER
	case "Election":
		primitiveType = primitiveapi.PrimitiveType_ELECTION
	case "IndexedMap":
		primitiveType = primitiveapi.PrimitiveType_INDEXED_MAP
	case "LeaderLatch":
		primitiveType = primitiveapi.PrimitiveType_LEADER_LATCH
	case "List":
		primitiveType = primitiveapi.PrimitiveType_LIST
	case "Lock":
		primitiveType = primitiveapi.PrimitiveType_LOCK
	case "Log":
		primitiveType = primitiveapi.PrimitiveType_LOG
	case "Map":
		primitiveType = primitiveapi.PrimitiveType_MAP
	case "Set":
		primitiveType = primitiveapi.PrimitiveType_SET
	case "Value":
		primitiveType = primitiveapi.PrimitiveType_VALUE
	}

	request := &primitiveapi.GetPrimitivesRequest{
		Protocol: &protocol.ProtocolId{
			Namespace: p.Namespace,
			Name:      p.Name,
		},
		Primitive: &primitiveapi.PrimitiveId{
			Namespace: p.Scope,
		},
		Type: primitiveType,
	}

	client := primitiveapi.NewPrimitiveServiceClient(p.conn)
	response, err := client.GetPrimitives(ctx, request)
	if err != nil {
		return nil, err
	}

	primitives := make([]primitive.Metadata, len(response.Primitives))
	for i, p := range response.Primitives {
		var primitiveType primitive.Type
		switch p.Type {
		case primitiveapi.PrimitiveType_COUNTER:
			primitiveType = "Counter"
		case primitiveapi.PrimitiveType_ELECTION:
			primitiveType = "Election"
		case primitiveapi.PrimitiveType_INDEXED_MAP:
			primitiveType = "IndexedMap"
		case primitiveapi.PrimitiveType_LEADER_LATCH:
			primitiveType = "LeaderLatch"
		case primitiveapi.PrimitiveType_LIST:
			primitiveType = "List"
		case primitiveapi.PrimitiveType_LOCK:
			primitiveType = "Lock"
		case primitiveapi.PrimitiveType_LOG:
			primitiveType = "Log"
		case primitiveapi.PrimitiveType_MAP:
			primitiveType = "Map"
		case primitiveapi.PrimitiveType_SET:
			primitiveType = "Set"
		case primitiveapi.PrimitiveType_VALUE:
			primitiveType = "Value"
		default:
			primitiveType = "Unknown"
		}
		primitives[i] = primitive.Metadata{
			Type: primitiveType,
			Name: primitive.Name{
				Namespace: p.Protocol.Namespace,
				Protocol:  p.Protocol.Name,
				Scope:     p.Primitive.Namespace,
				Name:      p.Primitive.Name,
			},
		}
	}
	return primitives, nil
}
