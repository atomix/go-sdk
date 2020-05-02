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

import "sync"

// NewEpochClock creates a new epoch clock
func NewEpochClock() Clock {
	return &EpochClock{
		timestamp: NewEpochTimestamp(NewEpoch(1), NewLogicalTimestamp(0)),
	}
}

type EpochClock struct {
	timestamp Timestamp
	mu        sync.RWMutex
}

func (c *EpochClock) New() Timestamp {
	return &LogicalTimestamp{}
}

func (c *EpochClock) Get() Timestamp {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp
}

func (c *EpochClock) Increment() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timestamp = NewEpochTimestamp(NewEpoch(c.timestamp.(*EpochTimestamp).epoch.value), NewLogicalTimestamp(c.timestamp.(*EpochTimestamp).timestamp.value+1))
	return c.timestamp
}

func (c *EpochClock) Update(timestamp Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	if timestamp.GreaterThan(c.timestamp) {
		c.timestamp = timestamp
	} else {
		c.timestamp = NewEpochTimestamp(NewEpoch(c.timestamp.(*EpochTimestamp).epoch.value), NewLogicalTimestamp(c.timestamp.(*EpochTimestamp).timestamp.value+1))
	}
	return c.timestamp
}

// NewEpochTimestamp creates a new epoch timestamp
func NewEpochTimestamp(epoch Timestamp, timestamp Timestamp) Timestamp {
	return &EpochTimestamp{
		epoch:     epoch.(*Epoch),
		timestamp: timestamp.(*LogicalTimestamp),
	}
}

// EpochTimestamp is a composite epoch + logical time timestamp
type EpochTimestamp struct {
	epoch     *Epoch
	timestamp *LogicalTimestamp
}

func (t *EpochTimestamp) Epoch() Timestamp {
	return t.epoch
}

func (t *EpochTimestamp) Timestamp() Timestamp {
	return t.timestamp
}

func (t *EpochTimestamp) GreaterThan(timestamp Timestamp) bool {
	if t.epoch.GreaterThan(timestamp.(*EpochTimestamp).epoch) {
		return true
	} else if t.epoch.EqualTo(timestamp.(*EpochTimestamp).epoch) && t.timestamp.GreaterThan(timestamp.(*EpochTimestamp).timestamp) {
		return true
	}
	return false
}

func (t *EpochTimestamp) GreaterThanOrEqualTo(timestamp Timestamp) bool {
	if t.epoch.GreaterThan(timestamp.(*EpochTimestamp).epoch) {
		return true
	} else if t.epoch.EqualTo(timestamp.(*EpochTimestamp).epoch) && t.timestamp.GreaterThanOrEqualTo(timestamp.(*EpochTimestamp).timestamp) {
		return true
	}
	return false
}

func (t *EpochTimestamp) LessThan(timestamp Timestamp) bool {
	if t.epoch.LessThan(timestamp.(*EpochTimestamp).epoch) {
		return true
	} else if t.epoch.EqualTo(timestamp.(*EpochTimestamp).epoch) && t.timestamp.LessThan(timestamp.(*EpochTimestamp).timestamp) {
		return true
	}
	return false
}

func (t *EpochTimestamp) LessThanOrEqualTo(timestamp Timestamp) bool {
	if t.epoch.LessThan(timestamp.(*EpochTimestamp).epoch) {
		return true
	} else if t.epoch.EqualTo(timestamp.(*EpochTimestamp).epoch) && t.timestamp.LessThanOrEqualTo(timestamp.(*EpochTimestamp).timestamp) {
		return true
	}
	return false
}

func (t *EpochTimestamp) EqualTo(timestamp Timestamp) bool {
	return t.epoch.EqualTo(timestamp.(*EpochTimestamp).epoch) && t.timestamp.EqualTo(timestamp.(*EpochTimestamp).timestamp)
}

func (t *EpochTimestamp) Marshal() ([]byte, error) {
	epochBytes, err := t.epoch.Marshal()
	if err != nil {
		return nil, err
	}
	timestampBytes, err := t.timestamp.Marshal()
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, len(epochBytes)+len(timestampBytes))
	i := 0
	for _, b := range epochBytes {
		bytes[i] = b
		i++
	}
	for _, b := range timestampBytes {
		bytes[i] = b
		i++
	}
	return bytes, nil
}

func (t *EpochTimestamp) Unmarshal(bytes []byte) error {
	epochBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		epochBytes[i] = bytes[i]
	}
	epoch := &EpochTimestamp{}
	err := epoch.Unmarshal(epochBytes)
	if err != nil {
		return err
	}

	timestampBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		timestampBytes[i] = bytes[i+8]
	}
	timestamp := &LogicalTimestamp{}
	err = timestamp.Unmarshal(timestampBytes)
	if err != nil {
		return err
	}
	return nil
}

func NewEpoch(epoch uint64) Timestamp {
	return &Epoch{
		LogicalTimestamp: &LogicalTimestamp{
			value: epoch,
		},
	}
}

type Epoch struct {
	*LogicalTimestamp
}
