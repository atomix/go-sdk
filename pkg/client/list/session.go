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

package list

import (
	"context"
	api "github.com/atomix/atomix-api/proto/atomix/list"
	"github.com/atomix/atomix-go-client/pkg/client/session"
)

type sessionHandler struct {
	client api.ListServiceClient
}

func (h *sessionHandler) Create(ctx context.Context, s *session.Session) error {
	request := &api.CreateRequest{
		Header:  s.GetState(),
		Timeout: &s.Timeout,
	}

	response, err := h.client.Create(ctx, request)
	if err != nil {
		return err
	}
	s.RecordResponse(request.Header, response.Header)
	return nil
}

func (h *sessionHandler) KeepAlive(ctx context.Context, s *session.Session) error {
	request := &api.KeepAliveRequest{
		Header: s.GetState(),
	}

	if _, err := h.client.KeepAlive(ctx, request); err != nil {
		return err
	}
	return nil
}

func (h *sessionHandler) Close(ctx context.Context, s *session.Session) error {
	request := &api.CloseRequest{
		Header: s.GetState(),
	}
	_, err := h.client.Close(ctx, request)
	return err
}

func (h *sessionHandler) Delete(ctx context.Context, s *session.Session) error {
	request := &api.CloseRequest{
		Header: s.GetState(),
		Delete: true,
	}
	_, err := h.client.Close(ctx, request)
	return err
}
