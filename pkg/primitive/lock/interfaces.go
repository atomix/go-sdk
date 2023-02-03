// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package lock

import (
	"context"
	"github.com/atomix/go-sdk/pkg/primitive"
)

type Builder interface {
	primitive.Builder[Builder, Lock]
}

// Lock provides distributed concurrency control
type Lock interface {
	primitive.Primitive

	// Lock acquires the lock
	Lock(ctx context.Context, opts ...LockOption) (Version, error)

	// Unlock releases the lock
	Unlock(ctx context.Context, opts ...UnlockOption) error

	// Get gets the lock status
	Get(ctx context.Context, opts ...GetOption) (Version, error)
}

type Version uint64
