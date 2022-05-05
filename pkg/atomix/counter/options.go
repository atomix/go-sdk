// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package counter

import (
	"github.com/atomix/go-client/pkg/atomix/primitive"
)

// Option is a counter option
type Option interface {
	primitive.Option
	applyNewCounter(options *newCounterOptions)
}

// newCounterOptions is counter options
type newCounterOptions struct{}
