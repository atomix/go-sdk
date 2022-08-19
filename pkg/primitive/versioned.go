// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

// Version is a version number for optimistic locking
type Version uint64

// Versioned is a versioned value
type Versioned[V any] struct {
	Version Version
	Value   V
}
