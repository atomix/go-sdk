// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package test

// Replica is an interface for implementing a test protocol replica
type Replica interface {
	// Start starts the replica
	Start() error
	// Stop stops the replica
	Stop() error
}

func newReplica(replica Replica) *testReplica {
	return &testReplica{
		Replica: replica,
	}
}

type testReplica struct {
	Replica
}
