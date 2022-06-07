// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"github.com/atomix/drivers/local"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"github.com/atomix/runtime/pkg/atomix/runtime"
)

func NewTest(kind primitive.Kind) *Test {
	return &Test{
		runtime: runtime.New(
			runtime.NewLocalNetwork(),
			runtime.WithDrivers(local.Driver),
			runtime.WithProxyKinds(kind)),
	}
}

type Test struct {
	runtime *runtime.Runtime
}

func (t *Test) Start() error {
	return t.runtime.Start()
}

func (t *Test) Controller() *Controller {
	return &Controller{t}
}

func (t *Test) Proxy() *Proxy {
	return &Proxy{t}
}

func (t *Test) Stop() error {
	return t.runtime.Stop()
}
