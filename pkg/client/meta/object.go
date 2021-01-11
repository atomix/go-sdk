// Copyright 2020-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	metaapi "github.com/atomix/api/go/atomix/primitive/meta"
)

// New creates new object metadata from the given proto metadata
func New(meta metaapi.ObjectMeta) ObjectMeta {
	var revision Revision
	if meta.Revision != nil {
		revision = Revision(meta.Revision.Num)
	}

	var timestamp Timestamp
	if meta.Timestamp != nil {
		switch t := meta.Timestamp.(type) {
		case *metaapi.ObjectMeta_PhysicalTimestamp:
			timestamp = NewWallClockTimestamp(WallClockTime(t.PhysicalTimestamp.Time))
		case *metaapi.ObjectMeta_LogicalTimestamp:
			timestamp = NewLogicalTimestamp(LogicalTime(t.LogicalTimestamp.Time))
		case *metaapi.ObjectMeta_VectorTimestamp:
			times := make([]LogicalTime, len(t.VectorTimestamp.Time))
			for i, time := range t.VectorTimestamp.Time {
				times[i] = LogicalTime(time)
			}
			timestamp = NewVectorTimestamp(times, 0)
		case *metaapi.ObjectMeta_EpochTimestamp:
			timestamp = NewEpochTimestamp(Epoch(t.EpochTimestamp.Epoch.Num), LogicalTime(t.EpochTimestamp.Sequence.Num))
		default:
			panic("unknown timestamp type")
		}
	}
	return ObjectMeta{
		Revision:  revision,
		Timestamp: timestamp,
	}
}

// ObjectMeta contains metadata about an object
type ObjectMeta struct {
	Revision Revision
	Timestamp
}

func (m ObjectMeta) Proto() metaapi.ObjectMeta {
	meta := metaapi.ObjectMeta{}
	if m.Revision > 0 {
		meta.Revision = &metaapi.Revision{
			Num: metaapi.RevisionNum(m.Revision),
		}
	}
	if m.Timestamp != nil {
		m.Timestamp.proto(&meta)
	}
	return meta
}

// Revision is a revision number
type Revision uint64
