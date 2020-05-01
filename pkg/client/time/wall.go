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
	"time"
)

func NewWallClockTimestamp() Timestamp {
	return &WallClockTimestamp{
		value: uint64(time.Now().UnixNano()),
	}
}

type WallClockTimestamp struct {
	value uint64
}

func (t *WallClockTimestamp) GreaterThan(timestamp Timestamp) bool {
	return t.value > timestamp.(*WallClockTimestamp).value
}

func (t *WallClockTimestamp) GreaterThanOrEqualTo(timestamp Timestamp) bool {
	return t.value >= timestamp.(*WallClockTimestamp).value
}

func (t *WallClockTimestamp) LessThan(timestamp Timestamp) bool {
	return t.value < timestamp.(*WallClockTimestamp).value
}

func (t *WallClockTimestamp) LessThanOrEqualTo(timestamp Timestamp) bool {
	return t.value <= timestamp.(*WallClockTimestamp).value
}

func (t *WallClockTimestamp) EqualTo(timestamp Timestamp) bool {
	return t.value == timestamp.(*WallClockTimestamp).value
}

func (t *WallClockTimestamp) Marshal() ([]byte, error) {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, t.value)
	return bytes, nil
}

func (t *WallClockTimestamp) Unmarshal(bytes []byte) error {
	t.value = binary.BigEndian.Uint64(bytes)
	return nil
}

func NewWallClock() Clock {
	return &WallClock{
		timestamp: &WallClockTimestamp{
			value: uint64(time.Now().UnixNano()),
		},
	}
}

type WallClock struct {
	timestamp Timestamp
	mu        sync.RWMutex
}

func (c *WallClock) New() Timestamp {
	return &WallClockTimestamp{}
}

func (c *WallClock) Get() Timestamp {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp
}

func (c *WallClock) Increment() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timestamp = NewWallClockTimestamp()
	return c.timestamp
}

func (c *WallClock) Update(timestamp Timestamp) Timestamp {
	return c.Increment()
}
