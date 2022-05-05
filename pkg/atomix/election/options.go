// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package election

import (
	"github.com/atomix/go-client/pkg/atomix/primitive"
)

// Option is a election option
type Option interface {
	primitive.Option
	applyNewElection(options *newElectionOptions)
}

// newElectionOptions is election options
type newElectionOptions struct{}
