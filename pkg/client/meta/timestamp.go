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
	"time"
)

// Timestamp is a request timestamp
type Timestamp interface {
	Before(Timestamp) bool
	After(Timestamp) bool
	Equal(Timestamp) bool
	proto(*metaapi.ObjectMeta)
}

type LogicalTime uint64

func NewLogicalTimestamp(time LogicalTime) Timestamp {
	return &LogicalTimestamp{
		Time: time,
	}
}

type LogicalTimestamp struct {
	Time LogicalTime
}

func (t LogicalTimestamp) Before(u Timestamp) bool {
	v, ok := u.(LogicalTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}
	return t.Time < v.Time
}

func (t LogicalTimestamp) After(u Timestamp) bool {
	v, ok := u.(LogicalTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}
	return t.Time > v.Time
}

func (t LogicalTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(LogicalTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}
	return t.Time == v.Time
}

func (t LogicalTimestamp) proto(meta *metaapi.ObjectMeta) {
	meta.Timestamp = &metaapi.ObjectMeta_LogicalTimestamp{
		LogicalTimestamp: &metaapi.LogicalTimestamp{
			Time: metaapi.LogicalTime(t.Time),
		},
	}
}

func NewVectorTimestamp(times []LogicalTime, i int) Timestamp {
	return &VectorTimestamp{
		Times: times,
		i:     i,
	}
}

type VectorTimestamp struct {
	Times []LogicalTime
	i     int
}

func (t VectorTimestamp) Before(u Timestamp) bool {
	v, ok := u.(VectorTimestamp)
	if !ok {
		panic("not a vector timestamp")
	}
	for i := range v.Times {
		if t.Times[t.i] >= v.Times[i] {
			return false
		}
	}
	return true
}

func (t VectorTimestamp) After(u Timestamp) bool {
	v, ok := u.(VectorTimestamp)
	if !ok {
		panic("not a vector timestamp")
	}
	for i := range v.Times {
		if t.Times[t.i] <= v.Times[i] {
			return false
		}
	}
	return true
}

func (t VectorTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(VectorTimestamp)
	if !ok {
		panic("not a vector timestamp")
	}
	for i := range v.Times {
		if t.Times[t.i] != v.Times[i] {
			return false
		}
	}
	return true
}

func (t VectorTimestamp) proto(meta *metaapi.ObjectMeta) {
	times := make([]metaapi.LogicalTime, len(t.Times))
	for i, time := range t.Times {
		times[i] = metaapi.LogicalTime(time)
	}
	meta.Timestamp = &metaapi.ObjectMeta_VectorTimestamp{
		VectorTimestamp: &metaapi.VectorTimestamp{
			Time: times,
		},
	}
}

type WallClockTime time.Time

func NewWallClockTimestamp(time WallClockTime) Timestamp {
	return &WallClockTimestamp{
		Time: time,
	}
}

type WallClockTimestamp struct {
	Time WallClockTime
}

func (t WallClockTimestamp) Before(u Timestamp) bool {
	v, ok := u.(WallClockTimestamp)
	if !ok {
		panic("not a wall clock timestamp")
	}
	return time.Time(t.Time).Before(time.Time(v.Time))
}

func (t WallClockTimestamp) After(u Timestamp) bool {
	v, ok := u.(WallClockTimestamp)
	if !ok {
		panic("not a wall clock timestamp")
	}
	return time.Time(t.Time).After(time.Time(v.Time))
}

func (t WallClockTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(WallClockTimestamp)
	if !ok {
		panic("not a wall clock timestamp")
	}
	return time.Time(t.Time).Equal(time.Time(v.Time))
}

func (t WallClockTimestamp) proto(meta *metaapi.ObjectMeta) {
	meta.Timestamp = &metaapi.ObjectMeta_PhysicalTimestamp{
		PhysicalTimestamp: &metaapi.PhysicalTimestamp{
			Time: metaapi.PhysicalTime(t.Time),
		},
	}
}

type Epoch uint64

func NewEpochTimestamp(epoch Epoch, time LogicalTime) Timestamp {
	return &EpochTimestamp{
		Epoch: epoch,
		Time:  time,
	}
}

type EpochTimestamp struct {
	Epoch Epoch
	Time  LogicalTime
}

func (t EpochTimestamp) Before(u Timestamp) bool {
	v, ok := u.(EpochTimestamp)
	if !ok {
		panic("not an epoch timestamp")
	}
	return t.Epoch < v.Epoch || (t.Epoch == v.Epoch && t.Time < v.Time)
}

func (t EpochTimestamp) After(u Timestamp) bool {
	v, ok := u.(EpochTimestamp)
	if !ok {
		panic("not an epoch timestamp")
	}
	return t.Epoch > v.Epoch || (t.Epoch == v.Epoch && t.Time > v.Time)
}

func (t EpochTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(EpochTimestamp)
	if !ok {
		panic("not an epoch timestamp")
	}
	return t.Epoch == v.Epoch && t.Time == v.Time
}

func (t EpochTimestamp) proto(meta *metaapi.ObjectMeta) {
	meta.Timestamp = &metaapi.ObjectMeta_EpochTimestamp{
		EpochTimestamp: &metaapi.EpochTimestamp{
			Epoch: metaapi.Epoch{
				Num: metaapi.EpochNum(t.Epoch),
			},
			Sequence: metaapi.Sequence{
				Num: metaapi.SequenceNum(t.Time),
			},
		},
	}
}
