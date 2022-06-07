// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
)

type Proxy struct {
	test *Test
}

func (t *Proxy) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	target := fmt.Sprintf(":%d", t.test.runtime.ProxyService.Port)
	return grpc.DialContext(ctx, target, grpc.WithContextDialer(t.test.runtime.Network().Connect))
}
