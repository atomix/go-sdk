// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import mapv1 "github.com/atomix/runtime/api/atomix/map/v1"

type testClient struct {
}

func (c *testClient) Map() mapv1.MapClient {
	return &testMapClient{}
}
