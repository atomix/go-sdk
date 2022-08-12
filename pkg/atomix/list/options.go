// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package list

import (
	listv1 "github.com/atomix/runtime/api/atomix/runtime/list/v1"
)

// EventsOption is an option for list Events calls
type EventsOption interface {
	beforeEvents(request *listv1.EventsRequest)
	afterEvents(response *listv1.EventsResponse)
}
