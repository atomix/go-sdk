// Copyright 2019-present Open Networking Foundation.
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

package time

import (
	"encoding/binary"
	"sync"
)

func NewLogicalTimestamp(value uint64) Timestamp {
	return &LogicalTimestamp{
		value: value,
	}
}

type LogicalTimestamp struct {
	value uint64
}

func (t *LogicalTimestamp) GreaterThan(timestamp Timestamp) bool {
	return t.value > timestamp.(*LogicalTimestamp).value
}

func (t *LogicalTimestamp) GreaterThanOrEqualTo(timestamp Timestamp) bool {
	return t.value >= timestamp.(*LogicalTimestamp).value
}

func (t *LogicalTimestamp) LessThan(timestamp Timestamp) bool {
	return t.value < timestamp.(*LogicalTimestamp).value
}

func (t *LogicalTimestamp) LessThanOrEqualTo(timestamp Timestamp) bool {
	return t.value <= timestamp.(*LogicalTimestamp).value
}

func (t *LogicalTimestamp) EqualTo(timestamp Timestamp) bool {
	return t.value == timestamp.(*LogicalTimestamp).value
}

func (t *LogicalTimestamp) Marshal() ([]byte, error) {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, t.value)
	return bytes, nil
}

func (t *LogicalTimestamp) Unmarshal(bytes []byte) error {
	t.value = binary.BigEndian.Uint64(bytes)
	return nil
}

func NewLogicalClock() Clock {
	return &LogicalClock{
		timestamp: NewLogicalTimestamp(0),
	}
}

type LogicalClock struct {
	timestamp Timestamp
	mu        sync.RWMutex
}

func (c *LogicalClock) New() Timestamp {
	return &LogicalTimestamp{}
}

func (c *LogicalClock) Get() Timestamp {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp
}

func (c *LogicalClock) Increment() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timestamp = NewLogicalTimestamp(c.timestamp.(*LogicalTimestamp).value + 1)
	return c.timestamp
}

func (c *LogicalClock) Update(timestamp Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	if timestamp.GreaterThan(c.timestamp) {
		c.timestamp = timestamp
	} else {
		c.timestamp = NewLogicalTimestamp(c.timestamp.(*LogicalTimestamp).value + 1)
	}
	return c.timestamp
}
