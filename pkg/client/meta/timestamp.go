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
	"time"
)

// Timestamp is a request timestamp
type Timestamp interface {
	Before(Timestamp) bool
	After(Timestamp) bool
	Equal(Timestamp) bool
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

type PhysicalTime time.Time

func NewPhysicalTimestamp(time PhysicalTime) Timestamp {
	return &PhysicalTimestamp{
		Time: time,
	}
}

type PhysicalTimestamp struct {
	Time PhysicalTime
}

func (t PhysicalTimestamp) Before(u Timestamp) bool {
	v, ok := u.(PhysicalTimestamp)
	if !ok {
		panic("not a wall clock timestamp")
	}
	return time.Time(t.Time).Before(time.Time(v.Time))
}

func (t PhysicalTimestamp) After(u Timestamp) bool {
	v, ok := u.(PhysicalTimestamp)
	if !ok {
		panic("not a wall clock timestamp")
	}
	return time.Time(t.Time).After(time.Time(v.Time))
}

func (t PhysicalTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(PhysicalTimestamp)
	if !ok {
		panic("not a wall clock timestamp")
	}
	return time.Time(t.Time).Equal(time.Time(v.Time))
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
