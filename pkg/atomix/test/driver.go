// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"context"
	"github.com/atomix/runtime/pkg/runtime"
)

func newDriver() runtime.Driver {
	return runtime.NewDriver[struct{}]("test", "v1", func(ctx context.Context, config struct{}) (runtime.Client, error) {

	})
}

var Driver = runtime.NewDriver[any]("test", "v1", func(ctx context.Context, config any) (runtime.Client, error) {
	return newTestClient()
})
